#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests
from xcures_toolkit.auth_common import get_xcures_bearer_token, load_env_file


DEFAULT_L10N_URL = (
    "https://raw.githubusercontent.com/HL7/cda-core-xsl/master/cda_l10n.xml"
)
REPO_ROOT = Path(__file__).resolve().parent.parent
FORCED_OUTPUT_DIR = REPO_ROOT / "downloads"


class PipelineError(RuntimeError):
    pass


def get_default_endpoint_template() -> str:
    base_url = (
        os.getenv("XCURES_BASE_URL", os.getenv("BASE_URL", "https://partner.xcures.com"))
        .strip()
        .rstrip("/")
    )
    return f"{base_url}/api/v1/patient-registry/document/{{documentId}}"


def get_base_url() -> str:
    return (
        os.getenv("XCURES_BASE_URL", os.getenv("BASE_URL", "https://partner.xcures.com"))
        .strip()
        .rstrip("/")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download an XML document from the get-document endpoint, apply cda2.xsl, "
            "and generate a PDF."
        )
    )
    parser.add_argument(
        "--document-id",
        help="Document ID used to call the endpoint.",
    )
    parser.add_argument(
        "--endpoint-template",
        default=get_default_endpoint_template(),
        help=(
            "Get-document endpoint URL template. Use {documentId} placeholder. "
            "Defaults to BASE_URL/ XCURES_BASE_URL + /api/v1/patient-registry/document/{documentId}. "
            "Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--xsl-path",
        default=str(Path(__file__).resolve().parent.parent / "configs" / "cda2.xsl"),
        help="Path to CDA XSL file. Default: %(default)s",
    )
    parser.add_argument(
        "--voc-file",
        help=(
            "Path to cda_l10n.xml. If not provided and missing next to the XSL, "
            "it is downloaded automatically."
        ),
    )
    parser.add_argument(
        "--l10n-url",
        default=DEFAULT_L10N_URL,
        help="Source URL for cda_l10n.xml auto-download. Default: %(default)s",
    )
    parser.add_argument(
        "--output-dir",
        default=str(FORCED_OUTPUT_DIR),
        help=(
            "Ignored. Outputs are always written to the repo-local downloads folder: "
            f"{FORCED_OUTPUT_DIR}."
        ),
    )
    parser.add_argument(
        "--renderer",
        choices=("auto", "wkhtmltopdf", "playwright", "weasyprint"),
        default="auto",
        help="PDF renderer. Default: %(default)s",
    )
    parser.add_argument(
        "--pdf-source",
        choices=("auto", "platform", "local"),
        default="auto",
        help=(
            "PDF generation source: platform endpoint or local XSL rendering. "
            "Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--keep-html",
        action="store_true",
        help="Keep intermediate transformed HTML file.",
    )
    parser.add_argument(
        "--bearer-token",
        default="",
        help=(
            "Optional bearer token override. If omitted, uses XCURES_BEARER_TOKEN "
            "or client-credentials auth from .env."
        ),
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Fail instead of prompting when --document-id is missing.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=120,
        help="HTTP timeout in seconds. Default: %(default)s",
    )
    return parser.parse_args()


def build_document_url(endpoint_template: str, document_id: str) -> str:
    if "{documentId}" in endpoint_template:
        return endpoint_template.replace("{documentId}", document_id)
    trimmed = endpoint_template.rstrip("/")
    return f"{trimmed}/{document_id}"


def build_headers(*, bearer_token: str, project_id: str) -> dict[str, str]:
    headers: dict[str, str] = {"Accept": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    if project_id:
        headers["ProjectId"] = project_id
    return headers


def resolve_bearer_token(explicit_token: str) -> str:
    token = explicit_token.strip()
    if token:
        return token
    try:
        return get_xcures_bearer_token()
    except Exception:
        return ""


def fail_for_bad_response(resp: requests.Response, context: str) -> None:
    if resp.ok:
        return
    snippet = (resp.text or "").strip().replace("\n", " ")
    if len(snippet) > 500:
        snippet = snippet[:500] + "..."
    raise PipelineError(f"{context} failed: HTTP {resp.status_code} body={snippet}")


def parse_json_or_raise(resp: requests.Response, context: str) -> Any:
    try:
        return resp.json()
    except ValueError as exc:
        raise PipelineError(f"{context} did not return JSON payload.") from exc


def find_first_value(payload: Any, keys: set[str]) -> Any:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in keys and value:
                return value
        for value in payload.values():
            found = find_first_value(value, keys)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_first_value(item, keys)
            if found:
                return found
    return None


def request_with_optional_refresh(
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    timeout_seconds: int,
    can_refresh_auth: bool,
) -> tuple[requests.Response, dict[str, str]]:
    with requests.Session() as session:
        resp = session.request(method, url, headers=headers, timeout=timeout_seconds)
        used_headers = dict(headers)
        if resp.status_code == 401 and can_refresh_auth:
            refreshed = resolve_bearer_token("")
            if refreshed:
                used_headers["Authorization"] = f"Bearer {refreshed}"
                resp = session.request(method, url, headers=used_headers, timeout=timeout_seconds)
    return resp, used_headers


def get_document_metadata(
    *,
    endpoint_url: str,
    headers: dict[str, str],
    can_refresh_auth: bool,
    timeout_seconds: int,
) -> Any:
    resp, _ = request_with_optional_refresh(
        method="GET",
        url=endpoint_url,
        headers=headers,
        timeout_seconds=timeout_seconds,
        can_refresh_auth=can_refresh_auth,
    )
    fail_for_bad_response(resp, f"GET {endpoint_url}")
    data = parse_json_or_raise(resp, f"GET {endpoint_url}")
    return data


def get_platform_pdf_metadata(
    *,
    base_url: str,
    document_id: str,
    headers: dict[str, str],
    can_refresh_auth: bool,
    timeout_seconds: int,
) -> tuple[requests.Response, Any]:
    url = f"{base_url}/api/patient-registry/document/{document_id}/pdf"
    resp, _ = request_with_optional_refresh(
        method="GET",
        url=url,
        headers=headers,
        timeout_seconds=timeout_seconds,
        can_refresh_auth=can_refresh_auth,
    )
    try:
        payload = resp.json()
    except ValueError:
        payload = None
    return resp, payload


def download_file(
    *,
    url: str,
    output_path: Path,
    timeout_seconds: int,
    headers: dict[str, str] | None = None,
) -> None:
    with requests.Session() as session:
        with session.get(url, headers=headers, stream=True, timeout=timeout_seconds) as resp:
            fail_for_bad_response(resp, f"GET {url}")
            with output_path.open("wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)


def ensure_voc_file(*, xsl_path: Path, provided_voc_file: str | None, l10n_url: str) -> Path:
    if provided_voc_file:
        path = Path(provided_voc_file).expanduser().resolve()
        if not path.exists():
            raise PipelineError(f"Provided --voc-file not found: {path}")
        return path

    default_path = xsl_path.parent / "cda_l10n.xml"
    if default_path.exists():
        return default_path

    with requests.Session() as session:
        resp = session.get(l10n_url, timeout=60)
    fail_for_bad_response(resp, f"Download cda_l10n.xml from {l10n_url}")
    default_path.write_bytes(resp.content)
    return default_path


def run_xslt_transform(*, xml_path: Path, xsl_path: Path, voc_path: Path, html_out: Path) -> None:
    if shutil.which("xsltproc") is None:
        raise PipelineError("xsltproc not found. Install libxslt/xsltproc first.")

    cmd = [
        "xsltproc",
        "--stringparam",
        "vocFile",
        voc_path.resolve().as_uri(),
        "--stringparam",
        "useJavascript",
        "false",
        "--output",
        str(html_out),
        str(xsl_path),
        str(xml_path),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise PipelineError(f"XSLT transform failed: {stderr}")


def render_with_wkhtmltopdf(*, html_path: Path, pdf_path: Path) -> None:
    binary = shutil.which("wkhtmltopdf")
    if not binary:
        raise PipelineError("wkhtmltopdf not found in PATH.")
    cmd = [
        binary,
        "--enable-local-file-access",
        str(html_path),
        str(pdf_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise PipelineError(f"wkhtmltopdf failed: {stderr}")


def render_with_weasyprint(*, html_path: Path, pdf_path: Path) -> None:
    if importlib.util.find_spec("weasyprint") is None:
        raise PipelineError("Python package weasyprint is not installed.")

    from weasyprint import HTML  # type: ignore

    HTML(filename=str(html_path), base_url=str(html_path.parent)).write_pdf(str(pdf_path))


def render_with_playwright(*, html_path: Path, pdf_path: Path) -> None:
    npx = shutil.which("npx")
    if not npx:
        raise PipelineError("npx not found. Install Node.js to use the playwright renderer.")

    cmd = [
        npx,
        "--yes",
        "playwright",
        "pdf",
        "--wait-for-timeout",
        "2000",
        html_path.resolve().as_uri(),
        str(pdf_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        message = stderr or stdout or "unknown playwright error"
        raise PipelineError(f"playwright pdf failed: {message}")


def render_pdf(*, html_path: Path, pdf_path: Path, renderer: str) -> str:
    if renderer == "wkhtmltopdf":
        render_with_wkhtmltopdf(html_path=html_path, pdf_path=pdf_path)
        return "wkhtmltopdf"
    if renderer == "playwright":
        render_with_playwright(html_path=html_path, pdf_path=pdf_path)
        return "playwright"
    if renderer == "weasyprint":
        render_with_weasyprint(html_path=html_path, pdf_path=pdf_path)
        return "weasyprint"

    try:
        render_with_wkhtmltopdf(html_path=html_path, pdf_path=pdf_path)
        return "wkhtmltopdf"
    except PipelineError:
        pass

    try:
        render_with_playwright(html_path=html_path, pdf_path=pdf_path)
        return "playwright"
    except PipelineError:
        pass

    render_with_weasyprint(html_path=html_path, pdf_path=pdf_path)
    return "weasyprint"


def make_output_stem(document_id: str, file_name: str | None) -> str:
    if file_name:
        name = Path(file_name).stem
        if name:
            return name
    return f"document_{document_id}"


def make_pdf_filename(value: str | None, fallback_stem: str) -> str:
    if value:
        name = Path(value).name.strip()
        if name:
            if not name.lower().endswith(".pdf"):
                name = f"{name}.pdf"
            return name
    return f"{fallback_stem}.pdf"


def resolve_document_id(cli_document_id: str | None, *, non_interactive: bool) -> str:
    value = (cli_document_id or "").strip()
    if value:
        return value
    if non_interactive:
        raise PipelineError("--document-id is required in non-interactive mode.")

    while True:
        entered = input("Enter documentId: ").strip()
        if entered:
            return entered
        print("documentId cannot be empty.", file=sys.stderr)


def main() -> int:
    load_env_file(REPO_ROOT / ".env")
    args = parse_args()

    try:
        document_id = resolve_document_id(
            args.document_id, non_interactive=args.non_interactive
        )
    except PipelineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    xsl_path = Path(args.xsl_path).expanduser().resolve()
    if not xsl_path.exists():
        print(f"error: xsl file not found: {xsl_path}", file=sys.stderr)
        return 2

    output_dir = FORCED_OUTPUT_DIR.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    endpoint_url = build_document_url(args.endpoint_template, document_id)
    base_url = get_base_url()
    project_id = os.getenv("XCURES_PROJECT_ID", "").strip()
    cli_token = (args.bearer_token or "").strip()
    env_token = os.getenv("XCURES_BEARER_TOKEN", "").strip()
    explicit_token = cli_token
    token = resolve_bearer_token(cli_token or env_token)
    headers = build_headers(
        bearer_token=token,
        project_id=project_id,
    )

    try:
        metadata = get_document_metadata(
            endpoint_url=endpoint_url,
            headers=headers,
            can_refresh_auth=(not explicit_token),
            timeout_seconds=args.timeout_seconds,
        )
        signed_s3_url = find_first_value(metadata, {"signedS3Url", "signedUrl"})
        if not isinstance(signed_s3_url, str) or not signed_s3_url.strip():
            raise PipelineError("Could not find signedS3Url/signedUrl in metadata payload.")
        if not signed_s3_url.lower().startswith(("http://", "https://")):
            raise PipelineError(
                f"Signed URL is not a valid HTTP URL: {signed_s3_url!r}. "
                f"Endpoint used: {endpoint_url}. "
                "This often means the docs mock/example endpoint was used."
            )

        file_name = find_first_value(metadata, {"fileName", "filename", "documentName", "name"})
        output_stem = make_output_stem(
            document_id, file_name if isinstance(file_name, str) else None
        )
        xml_path = output_dir / f"{output_stem}.xml"
        default_html_path = output_dir / f"{output_stem}.html"
        default_pdf_path = output_dir / f"{output_stem}.pdf"

        download_file(
            url=signed_s3_url,
            output_path=xml_path,
            timeout_seconds=args.timeout_seconds,
        )

        if args.pdf_source in {"auto", "platform"}:
            platform_resp, platform_payload = get_platform_pdf_metadata(
                base_url=base_url,
                document_id=document_id,
                headers=headers,
                can_refresh_auth=(not explicit_token),
                timeout_seconds=args.timeout_seconds,
            )
            if platform_resp.ok:
                platform_signed_url = find_first_value(platform_payload, {"signedUrl", "signedS3Url"})
                if isinstance(platform_signed_url, str) and platform_signed_url.strip():
                    platform_file_name = find_first_value(
                        platform_payload, {"fileName", "filename", "name"}
                    )
                    pdf_name = make_pdf_filename(
                        platform_file_name if isinstance(platform_file_name, str) else None,
                        output_stem,
                    )
                    pdf_path = output_dir / pdf_name
                    download_file(
                        url=platform_signed_url,
                        output_path=pdf_path,
                        timeout_seconds=args.timeout_seconds,
                    )
                    print(f"Metadata URL: {endpoint_url}")
                    print(f"Downloaded XML: {xml_path}")
                    print(f"PDF source: platform")
                    print(f"Generated PDF: {pdf_path}")
                    return 0
                if args.pdf_source == "platform":
                    raise PipelineError(
                        "Platform PDF metadata missing signedUrl/signedS3Url."
                    )
            elif args.pdf_source == "platform":
                snippet = (platform_resp.text or "").strip().replace("\n", " ")
                if len(snippet) > 500:
                    snippet = snippet[:500] + "..."
                raise PipelineError(
                    f"Platform PDF endpoint failed: HTTP {platform_resp.status_code} body={snippet}"
                )

        if args.pdf_source == "platform":
            raise PipelineError(
                "Platform PDF endpoint unavailable. "
                "Set XCURES_BEARER_TOKEN with a token that can access /api/patient-registry/document/{id}/pdf."
            )

        voc_path = ensure_voc_file(
            xsl_path=xsl_path,
            provided_voc_file=args.voc_file,
            l10n_url=args.l10n_url,
        )

        run_xslt_transform(
            xml_path=xml_path,
            xsl_path=xsl_path,
            voc_path=voc_path,
            html_out=default_html_path,
        )
        renderer_used = render_pdf(
            html_path=default_html_path,
            pdf_path=default_pdf_path,
            renderer=args.renderer,
        )

        if not args.keep_html and default_html_path.exists():
            default_html_path.unlink()

        print(f"Metadata URL: {endpoint_url}")
        print(f"Downloaded XML: {xml_path}")
        print(f"PDF source: local ({renderer_used})")
        print(f"Generated PDF: {default_pdf_path}")
        return 0
    except PipelineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        if args.renderer == "auto" and (
            "wkhtmltopdf" in str(exc)
            or "weasyprint" in str(exc)
            or "playwright" in str(exc)
        ):
            print(
                "hint: Install wkhtmltopdf, or run "
                "`npx playwright install chromium`, or install weasyprint "
                "(python3 -m pip install weasyprint).",
                file=sys.stderr,
            )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
