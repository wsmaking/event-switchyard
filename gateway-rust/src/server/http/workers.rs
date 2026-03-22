use super::tcp::run_v3_tcp_server;
use super::*;
use std::future::Future;

pub(super) fn parse_cpu_affinity_list_env(key: &str) -> Vec<usize> {
    let raw = match std::env::var(key) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let mut cpus = Vec::new();
    for part in raw.split(',') {
        let token = part.trim();
        if token.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = token.split_once('-') {
            let start = lo.trim().parse::<usize>().ok();
            let end = hi.trim().parse::<usize>().ok();
            let (Some(start), Some(end)) = (start, end) else {
                continue;
            };
            if start <= end {
                cpus.extend(start..=end);
            } else {
                cpus.extend(end..=start);
            }
            continue;
        }
        if let Ok(cpu) = token.parse::<usize>() {
            cpus.push(cpu);
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    cpus
}

#[inline]
pub(super) fn affinity_cpu_for_worker(cpus: &[usize], idx: usize) -> Option<usize> {
    if cpus.is_empty() {
        None
    } else {
        Some(cpus[idx % cpus.len()])
    }
}

#[cfg(target_os = "linux")]
fn pin_current_thread_cpu(cpu: usize) -> std::io::Result<()> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
    }
    let rc = unsafe {
        libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &set as *const libc::cpu_set_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread_cpu(_cpu: usize) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "thread affinity is only supported on Linux",
    ))
}

fn spawn_v3_runtime_thread<Fut>(
    name: String,
    cpu: Option<usize>,
    affinity_ok_total: Arc<AtomicU64>,
    affinity_err_total: Arc<AtomicU64>,
    fut: Fut,
) where
    Fut: Future<Output = ()> + Send + 'static,
{
    let thread_name = name.clone();
    let affinity_err_total_spawn = affinity_err_total.clone();
    let spawn_result = std::thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
            if let Some(cpu) = cpu {
                match pin_current_thread_cpu(cpu) {
                    Ok(()) => {
                        affinity_ok_total.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) => {
                        affinity_err_total_spawn.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(thread = %thread_name, cpu = cpu, error = %err, "failed to pin v3 worker thread");
                    }
                }
            }
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt.block_on(fut),
                Err(err) => tracing::error!(thread = %thread_name, error = %err, "failed to build dedicated v3 runtime"),
            }
        });
    if let Err(err) = spawn_result {
        affinity_err_total.fetch_add(1, Ordering::Relaxed);
        tracing::error!(thread = %name, error = %err, "failed to spawn dedicated v3 worker thread");
    }
}

pub(super) fn log_v3_worker_topology(
    state: &AppState,
    v3_shard_affinity_cpus: &[usize],
    v3_durable_affinity_cpus: &[usize],
    v3_tcp_server_affinity_cpus: &[usize],
    v3_dedicated_worker_runtime: bool,
) {
    tracing::info!(
        shard_affinity = ?v3_shard_affinity_cpus,
        durable_affinity = ?v3_durable_affinity_cpus,
        tcp_server_affinity = ?v3_tcp_server_affinity_cpus,
        tsc_enabled = state.v3_tsc_runtime_enabled.load(Ordering::Relaxed),
        tsc_hz = state.v3_tsc_hz,
        tsc_invariant = state.v3_tsc_invariant,
        hotpath_histogram_sample_rate = state.v3_hotpath_histogram_sample_rate,
        dedicated_worker_runtime = v3_dedicated_worker_runtime,
        "v3 worker topology tuning"
    );
}

pub(super) fn spawn_v3_background_workers(
    state: &AppState,
    v3_rxs: Vec<Receiver<V3OrderTask>>,
    v3_durable_rxs: Vec<Receiver<V3DurableTask>>,
    durable_rx: Option<UnboundedReceiver<AuditDurableNotification>>,
    v3_shard_affinity_cpus: &[usize],
    v3_durable_affinity_cpus: &[usize],
    v3_dedicated_worker_runtime: bool,
    v3_durable_worker_batch_adaptive_cfg: V3DurableWorkerBatchAdaptiveConfig,
    v3_durable_worker_pressure_cfg: V3DurableWorkerPressureConfig,
) {
    for (shard_id, v3_rx) in v3_rxs.into_iter().enumerate() {
        let writer_state = state.clone();
        let affinity_cpu = affinity_cpu_for_worker(v3_shard_affinity_cpus, shard_id);
        if v3_dedicated_worker_runtime {
            spawn_v3_runtime_thread(
                format!("v3-shard-{shard_id}"),
                affinity_cpu,
                Arc::clone(&state.v3_thread_affinity_apply_success_total),
                Arc::clone(&state.v3_thread_affinity_apply_failure_total),
                async move {
                    run_v3_single_writer(shard_id, v3_rx, writer_state).await;
                },
            );
        } else {
            tokio::spawn(async move {
                run_v3_single_writer(shard_id, v3_rx, writer_state).await;
            });
        }
    }

    for (lane_id, v3_durable_rx) in v3_durable_rxs.into_iter().enumerate() {
        let durable_state = state.clone();
        let durable_batch_max = durable_state.v3_durable_worker_batch_max;
        let durable_batch_wait_us = durable_state.v3_durable_worker_batch_wait_us;
        let durable_batch_adaptive_cfg = v3_durable_worker_batch_adaptive_cfg;
        let durable_pressure_cfg = v3_durable_worker_pressure_cfg;
        let affinity_cpu = affinity_cpu_for_worker(v3_durable_affinity_cpus, lane_id);
        if v3_dedicated_worker_runtime {
            spawn_v3_runtime_thread(
                format!("v3-durable-{lane_id}"),
                affinity_cpu,
                Arc::clone(&state.v3_thread_affinity_apply_success_total),
                Arc::clone(&state.v3_thread_affinity_apply_failure_total),
                async move {
                    run_v3_durable_worker(
                        lane_id,
                        v3_durable_rx,
                        durable_state,
                        durable_batch_max,
                        durable_batch_wait_us,
                        durable_batch_adaptive_cfg,
                        durable_pressure_cfg,
                    )
                    .await;
                },
            );
        } else {
            tokio::spawn(async move {
                run_v3_durable_worker(
                    lane_id,
                    v3_durable_rx,
                    durable_state,
                    durable_batch_max,
                    durable_batch_wait_us,
                    durable_batch_adaptive_cfg,
                    durable_pressure_cfg,
                )
                .await;
            });
        }
    }

    let loss_state = state.clone();
    tokio::spawn(async move {
        run_v3_loss_monitor(loss_state).await;
    });

    if let Some(rx) = durable_rx {
        let durable_state = state.clone();
        tokio::spawn(async move {
            run_durable_notifier(rx, durable_state).await;
        });
    }
}

pub(super) fn spawn_v3_tcp_ingress(
    state: &AppState,
    v3_tcp_enable: bool,
    v3_tcp_port: u16,
    v3_dedicated_worker_runtime: bool,
    v3_tcp_server_affinity_cpus: &[usize],
) {
    tracing::info!(
        v3_tcp_enable = v3_tcp_enable,
        v3_tcp_port = v3_tcp_port,
        "v3 tcp ingress settings"
    );
    if !(v3_tcp_enable && v3_tcp_port > 0) {
        return;
    }
    let v3_tcp_state = state.clone();
    let affinity_cpu = v3_tcp_server_affinity_cpus.first().copied();
    if v3_dedicated_worker_runtime && affinity_cpu.is_some() {
        spawn_v3_runtime_thread(
            "v3-tcp-ingress".to_string(),
            affinity_cpu,
            Arc::clone(&state.v3_thread_affinity_apply_success_total),
            Arc::clone(&state.v3_thread_affinity_apply_failure_total),
            async move {
                if let Err(err) = run_v3_tcp_server(v3_tcp_port, v3_tcp_state).await {
                    tracing::error!(error = %err, "v3 tcp server exited");
                }
            },
        );
    } else {
        if !v3_dedicated_worker_runtime && affinity_cpu.is_some() {
            tracing::warn!(
                "V3_TCP_SERVER_AFFINITY_CPUS is ignored when V3_DEDICATED_WORKER_RUNTIME=false"
            );
        }
        tokio::spawn(async move {
            if let Err(err) = run_v3_tcp_server(v3_tcp_port, v3_tcp_state).await {
                tracing::error!(error = %err, "v3 tcp server exited");
            }
        });
    }
}
