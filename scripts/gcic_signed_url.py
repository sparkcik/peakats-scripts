"""
gcic_signed_url.py
Reusable helper: generates a 5-day signed URL for a GCIC PDF in Supabase Storage.
Import into any report script:
    from gcic_signed_url import get_gcic_signed_url
"""

import os, requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://eyopvsmsvbgfuffscfom.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
STORAGE_BUCKET = "gcic-docs"
SIGNED_URL_EXPIRY = 432000  # 5 days in seconds


def get_gcic_signed_url(gcic_signed_pdf_path, fallback_drive_link=None):
    """
    Generate a 5-day signed URL for the GCIC PDF in Supabase Storage.

    Args:
        gcic_signed_pdf_path: Storage path from candidates.gcic_signed_pdf_path
                              e.g. "cbm/123_Smith_GCIC.pdf"
        fallback_drive_link:  Optional Google Drive link to fall back to

    Returns:
        Signed URL string, or fallback_drive_link if path is empty/fails
    """
    if not gcic_signed_pdf_path:
        return fallback_drive_link

    key = SUPABASE_KEY or os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not key:
        return fallback_drive_link

    try:
        url = f"{SUPABASE_URL}/storage/v1/object/sign/{STORAGE_BUCKET}/{gcic_signed_pdf_path}"
        r = requests.post(url,
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            json={"expiresIn": SIGNED_URL_EXPIRY},
            timeout=10,
        )
        r.raise_for_status()
        signed_path = r.json().get("signedURL", "")
        if signed_path:
            # signedURL is relative, prepend base URL
            if signed_path.startswith("/"):
                return f"{SUPABASE_URL}/storage/v1{signed_path}"
            return signed_path
    except Exception as e:
        print(f"[gcic_signed_url] Error generating signed URL: {e}")

    return fallback_drive_link


if __name__ == "__main__":
    # Quick test
    import sys
    if len(sys.argv) > 1:
        path = sys.argv[1]
        url = get_gcic_signed_url(path)
        print(f"Path: {path}")
        print(f"URL:  {url}")
    else:
        print("Usage: SUPABASE_KEY=... python3 gcic_signed_url.py <storage_path>")
        print("Example: python3 gcic_signed_url.py cbm/123_Smith_GCIC.pdf")
