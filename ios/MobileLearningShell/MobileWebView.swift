import SwiftUI
import WebKit

struct MobileWebView: UIViewRepresentable {
    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeUIView(context: Context) -> WKWebView {
        let configuration = WKWebViewConfiguration()
        configuration.defaultWebpagePreferences.allowsContentJavaScript = true
        configuration.userContentController.add(context.coordinator, name: Coordinator.logHandlerName)
        configuration.userContentController.addUserScript(
            WKUserScript(
                source: Coordinator.debugBridgeScript,
                injectionTime: .atDocumentStart,
                forMainFrameOnly: true
            )
        )

        let webView = WKWebView(frame: .zero, configuration: configuration)
        webView.navigationDelegate = context.coordinator
        webView.uiDelegate = context.coordinator
        webView.scrollView.contentInsetAdjustmentBehavior = .never
        if #available(iOS 16.4, *) {
            webView.isInspectable = true
        }
        loadEntry(on: webView)
        return webView
    }

    func updateUIView(_ webView: WKWebView, context: Context) {
        if webView.url == nil {
            loadEntry(on: webView)
        }
    }

    private func loadEntry(on webView: WKWebView) {
        guard let entryURL = Bundle.main.url(forResource: "index", withExtension: "html", subdirectory: "WebApp") else {
            webView.loadHTMLString(
                """
                <html>
                <body style="background:#08111d;color:#e2e8f0;font-family:-apple-system;padding:24px;">
                  <h2>web_bundle_missing</h2>
                  <p>Run scripts/ops/prepare_mobile_ios_shell.sh before launching the iOS shell.</p>
                </body>
                </html>
                """,
                baseURL: nil
            )
            return
        }

        let readAccessURL = entryURL.deletingLastPathComponent()
        do {
            let html = try String(contentsOf: entryURL, encoding: .utf8)
            let needsRewrite = html.contains("href=\"/") || html.contains("src=\"/") || html.contains("crossorigin")
            if needsRewrite {
                let normalizedHtml = html
                    .replacingOccurrences(of: "<head>", with: "<head><base href=\"./\">")
                    .replacingOccurrences(of: " crossorigin=\"\"", with: "")
                    .replacingOccurrences(of: " crossorigin", with: "")
                    .replacingOccurrences(of: "href=\"/", with: "href=\"./")
                    .replacingOccurrences(of: "src=\"/", with: "src=\"./")
                webView.loadHTMLString(normalizedHtml, baseURL: readAccessURL)
            } else {
                webView.loadFileURL(entryURL, allowingReadAccessTo: readAccessURL)
            }
        } catch {
            webView.loadFileURL(entryURL, allowingReadAccessTo: readAccessURL)
        }
    }

    final class Coordinator: NSObject, WKNavigationDelegate, WKUIDelegate, WKScriptMessageHandler {
        static let logHandlerName = "mobileLog"
        static let debugBridgeScript = """
        (function () {
          if (window.__mobileShellDebugInstalled) {
            return;
          }
          window.__mobileShellDebugInstalled = true;
          function stringify(value) {
            if (typeof value === 'string') return value;
            try { return JSON.stringify(value); } catch (_) { return String(value); }
          }
          function post(kind, payload) {
            try {
              window.webkit.messageHandlers.mobileLog.postMessage({
                kind: kind,
                payload: stringify(payload),
                href: String(location.href)
              });
            } catch (_) {}
          }
          ['log', 'warn', 'error'].forEach(function (level) {
            var original = console[level];
            console[level] = function () {
              post('console.' + level, Array.prototype.slice.call(arguments).map(stringify).join(' '));
              if (original) {
                original.apply(console, arguments);
              }
            };
          });
          window.addEventListener('error', function (event) {
            post('window.error', {
              message: event.message,
              source: event.filename,
              line: event.lineno,
              column: event.colno
            });
          });
          window.addEventListener('error', function (event) {
            var target = event.target;
            if (target && target !== window) {
              post('resource.error', {
                tagName: target.tagName,
                src: target.src || target.href || null,
                outerHTML: target.outerHTML ? String(target.outerHTML).slice(0, 240) : null
              });
            }
          }, true);
          window.addEventListener('unhandledrejection', function (event) {
            var reason = event.reason;
            post('window.unhandledrejection', reason && (reason.stack || reason.message || reason));
          });
          document.addEventListener('DOMContentLoaded', function () {
            post('dom.contentLoaded', {
              title: document.title,
              rootExists: !!document.getElementById('root'),
              scriptCount: document.scripts.length
            });
          });
          function sampleRoot(label) {
            var root = document.getElementById('root');
            post('root.sample', {
              label: label,
              hasRoot: !!root,
              childCount: root ? root.childElementCount : null,
              textLength: root && root.textContent ? root.textContent.length : 0,
              htmlLength: root && root.innerHTML ? root.innerHTML.length : 0
            });
          }
          setTimeout(function () { sampleRoot('t+250ms'); }, 250);
          setTimeout(function () { sampleRoot('t+1500ms'); }, 1500);
          post('bootstrap', 'debug bridge ready');
        })();
        """

        func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
            guard message.name == Self.logHandlerName else {
                return
            }
            print("[MobileShell][JS]", message.body)
        }

        func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation!) {
            print("[MobileShell] didStartProvisionalNavigation", webView.url?.absoluteString ?? "nil")
        }

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            print("[MobileShell] didFinish", webView.url?.absoluteString ?? "nil")
            webView.evaluateJavaScript("""
            JSON.stringify({
              href: location.href,
              title: document.title,
              rootHtmlLength: document.getElementById('root') ? document.getElementById('root').innerHTML.length : -1,
              scripts: Array.from(document.scripts).map(function (s) { return { src: s.src, type: s.type || 'classic' }; })
            })
            """) { value, error in
                if let error {
                    print("[MobileShell] evaluateJavaScript error", error.localizedDescription)
                    return
                }
                print("[MobileShell] domSnapshot", value ?? "nil")
            }
        }

        func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
            print("[MobileShell] didFail", error.localizedDescription)
        }

        func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
            print("[MobileShell] didFailProvisionalNavigation", error.localizedDescription)
        }

        func webViewWebContentProcessDidTerminate(_ webView: WKWebView) {
            print("[MobileShell] webContentProcessDidTerminate")
        }

        func webView(
            _ webView: WKWebView,
            decidePolicyFor navigationAction: WKNavigationAction,
            decisionHandler: @escaping (WKNavigationActionPolicy) -> Void
        ) {
            guard let url = navigationAction.request.url else {
                decisionHandler(.allow)
                return
            }

            if url.isFileURL || url.scheme == "about" {
                decisionHandler(.allow)
                return
            }

            if url.scheme == "http" || url.scheme == "https" {
                UIApplication.shared.open(url)
                decisionHandler(.cancel)
                return
            }

            decisionHandler(.allow)
        }

        func webView(
            _ webView: WKWebView,
            createWebViewWith configuration: WKWebViewConfiguration,
            for navigationAction: WKNavigationAction,
            windowFeatures: WKWindowFeatures
        ) -> WKWebView? {
            if navigationAction.targetFrame == nil, let url = navigationAction.request.url {
                UIApplication.shared.open(url)
            }
            return nil
        }
    }
}
