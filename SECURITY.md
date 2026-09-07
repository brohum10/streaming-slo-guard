# Security Policy

Streaming SLO Guard processes untrusted telemetry streams. Parser denial of service, memory-bound bypasses, unsafe path handling, or crafted inputs that suppress or fabricate alerts should be reported privately.

Use GitHub's **Security → Report a vulnerability** flow when available. Otherwise, contact the maintainer through the GitHub profile. Include a sanitized minimal event stream, affected configuration, impact, and expected behavior.

The current `main` branch is supported. The project is a monitoring engine rather than a complete ingestion service; deployments remain responsible for authentication, transport security, tenant isolation, and retention controls.
