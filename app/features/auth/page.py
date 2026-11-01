"""The page a player's browser shows after Discord sign-in. The bot carries on
in Discord, so the page only says how it went and to go back."""

from __future__ import annotations

from html import escape

from fastapi.responses import HTMLResponse

# Nothing loads from elsewhere, no site can frame the page, and the address
# (which holds the sign-in code) is never sent on.
HEADERS = {
    "Cache-Control": "no-store",
    "Content-Security-Policy": (
        "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'"
    ),
    "Referrer-Policy": "no-referrer",
}

TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex">
<title>{title}</title>
<style>
body {{ margin: 0; min-height: 100vh; display: grid; place-items: center;
  font: 16px/1.5 system-ui, sans-serif; background: #f4f5f7; color: #1e1f22; }}
main {{ max-width: 28rem; margin: 1rem; padding: 1.5rem 2rem; border-radius: 12px;
  background: #fff; box-shadow: 0 2px 12px rgb(0 0 0 / 0.08); }}
h1 {{ font-size: 1.3rem; margin: 0 0 0.5rem; }}
.ref {{ font-size: 0.85rem; color: #5c5e66; }}
@media (prefers-color-scheme: dark) {{
  body {{ background: #1e1f22; color: #f2f3f5; }}
  main {{ background: #2b2d31; }}
  .ref {{ color: #b5bac1; }}
}}
</style>
</head>
<body>
<main>
<h1>{title}</h1>
<p>{message}</p>
{ref}<p>You can close this tab and go back to Discord.</p>
</main>
</body>
</html>
"""


def sign_in_page(
    status_code: int, title: str, message: str, ref: str | None = None
) -> HTMLResponse:
    reference = f'<p class="ref">Reference: {escape(ref)}</p>\n' if ref else ""
    html = TEMPLATE.format(title=escape(title), message=escape(message), ref=reference)
    return HTMLResponse(html, status_code=status_code, headers=HEADERS)
