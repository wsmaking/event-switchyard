import SwiftUI
import WebKit

struct MobileWebView: UIViewRepresentable {
    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeUIView(context: Context) -> WKWebView {
        let configuration = WKWebViewConfiguration()
        configuration.defaultWebpagePreferences.allowsContentJavaScript = true

        let webView = WKWebView(frame: .zero, configuration: configuration)
        webView.navigationDelegate = context.coordinator
        webView.uiDelegate = context.coordinator
        webView.scrollView.contentInsetAdjustmentBehavior = .never
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
            let normalizedHtml = html
                .replacingOccurrences(of: "href=\"/", with: "href=\"./")
                .replacingOccurrences(of: "src=\"/", with: "src=\"./")
            webView.loadHTMLString(normalizedHtml, baseURL: readAccessURL)
        } catch {
            webView.loadFileURL(entryURL, allowingReadAccessTo: readAccessURL)
        }
    }

    final class Coordinator: NSObject, WKNavigationDelegate, WKUIDelegate {
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
