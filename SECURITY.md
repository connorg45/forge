# Security policy

Please do not open public issues for suspected vulnerabilities. Send a private report through GitHub Security Advisories with the affected version, reproduction steps, impact, and any proposed mitigation. Secrets, credentials, and production data must never be included in reports or fixtures.

Forge is a portfolio project and does not include application-level authentication or tenant authorization. The supplied Compose environment is for local evaluation only: published ports are loopback-only, credentials are development values, and Grafana permits anonymous administration. Operators must add authenticated TLS termination, restrict every service to trusted networks, configure exact CORS origins, manage secrets, back up PostgreSQL, and keep dependencies updated before any non-local deployment.
