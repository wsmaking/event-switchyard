import SwiftUI
import WebKit

struct MobileWebView: UIViewRepresentable {
    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeUIView(context: Context) -> WKWebView {
        let configuration = WKWebViewConfiguration()
        configuration.defaultWebpagePreferences.allowsContentJavaScript = true
        configuration.setURLSchemeHandler(AppBundleSchemeHandler(), forURLScheme: AppBundleSchemeHandler.scheme)
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
        guard Bundle.main.url(forResource: "index", withExtension: "html", subdirectory: "WebApp") != nil else {
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
        if let entryURL = URL(string: "\(AppBundleSchemeHandler.scheme)://webapp/index.html") {
            webView.load(URLRequest(url: entryURL))
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

            if url.scheme == AppBundleSchemeHandler.scheme || url.isFileURL || url.scheme == "about" {
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

final class AppBundleSchemeHandler: NSObject, WKURLSchemeHandler {
    static let scheme = "appbundle"

    func webView(_ webView: WKWebView, start urlSchemeTask: WKURLSchemeTask) {
        guard let requestURL = urlSchemeTask.request.url else {
            urlSchemeTask.didFailWithError(AppBundleSchemeError.invalidURL)
            return
        }

        do {
            let fileURL = try resolveBundleURL(for: requestURL)
            let data = try Data(contentsOf: fileURL)
            let response = URLResponse(
                url: requestURL,
                mimeType: mimeType(for: fileURL.pathExtension),
                expectedContentLength: data.count,
                textEncodingName: textEncodingName(for: fileURL.pathExtension)
            )
            urlSchemeTask.didReceive(response)
            urlSchemeTask.didReceive(data)
            urlSchemeTask.didFinish()
        } catch {
            urlSchemeTask.didFailWithError(error)
        }
    }

    func webView(_ webView: WKWebView, stop urlSchemeTask: WKURLSchemeTask) {
    }

    private func resolveBundleURL(for requestURL: URL) throws -> URL {
        guard var webAppURL = Bundle.main.resourceURL?.appendingPathComponent("WebApp", isDirectory: true) else {
            throw AppBundleSchemeError.bundleMissing
        }

        let path = requestURL.path.isEmpty || requestURL.path == "/" ? "/index.html" : requestURL.path
        for component in path.split(separator: "/") {
            guard component != ".." else {
                throw AppBundleSchemeError.invalidPath
            }
            webAppURL.appendPathComponent(String(component), isDirectory: false)
        }

        guard FileManager.default.fileExists(atPath: webAppURL.path) else {
            throw AppBundleSchemeError.fileNotFound(webAppURL.lastPathComponent)
        }
        return webAppURL
    }

    private func mimeType(for pathExtension: String) -> String {
        switch pathExtension.lowercased() {
        case "html":
            return "text/html"
        case "js", "mjs":
            return "application/javascript"
        case "css":
            return "text/css"
        case "json", "webmanifest":
            return "application/json"
        case "svg":
            return "image/svg+xml"
        case "png":
            return "image/png"
        case "jpg", "jpeg":
            return "image/jpeg"
        case "gif":
            return "image/gif"
        case "woff":
            return "font/woff"
        case "woff2":
            return "font/woff2"
        default:
            return "application/octet-stream"
        }
    }

    private func textEncodingName(for pathExtension: String) -> String? {
        switch pathExtension.lowercased() {
        case "html", "js", "mjs", "css", "json", "webmanifest", "svg":
            return "utf-8"
        default:
            return nil
        }
    }
}

enum AppBundleSchemeError: LocalizedError {
    case invalidURL
    case invalidPath
    case bundleMissing
    case fileNotFound(String)

    var errorDescription: String? {
        switch self {
        case .invalidURL:
            return "invalid_appbundle_url"
        case .invalidPath:
            return "invalid_appbundle_path"
        case .bundleMissing:
            return "webapp_bundle_missing"
        case .fileNotFound(let name):
            return "webapp_file_not_found:\(name)"
        }
    }
}
