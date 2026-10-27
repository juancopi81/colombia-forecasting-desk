"""Verified certificate-chain completion for BanRep's SUAMECA host."""
from __future__ import annotations

from importlib import resources
import ssl


def banrep_ssl_context() -> ssl.SSLContext:
    # SUAMECA's September 11, 2026 leaf omits this public intermediate. It is
    # signed by DigiCert Global Root G2, already in the default trusted roots.
    # No leaf pinning, partial-chain flag, disabled verification or network I/O.
    context = ssl.create_default_context()
    context.verify_flags &= ~ssl.VERIFY_X509_PARTIAL_CHAIN
    issuer = resources.files("colombia_forecasting_desk").joinpath(
        "data/geotrust-ev-rsa-ca-g2.pem"
    ).read_text(encoding="ascii")
    context.load_verify_locations(cadata=issuer)
    return context
