# Security Policy

## Supported Versions

We actively support the latest major and minor versions of stacks-data. Security updates are provided for:

- Latest release (currently 2.x)
- Previous minor version for critical vulnerabilities

## Reporting a Vulnerability

If you discover a security vulnerability in stacks-data, please report it to:

- **Email:** stacks@ensono.com
- **Subject:** [SECURITY] Description of the issue

Please include:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested remediation (if any)

We aim to respond to security reports within 48 hours.

## Known Security Considerations

### nbconvert Windows Vulnerability (CVE-2025-53000)

**Status:** No patch available  
**Affected Component:** nbconvert ≤ 7.16.6 (transitive dependency via great-expectations)  
**Platform:** Windows only  
**Severity:** High (CVSS 8.5)

#### Description

nbconvert has an uncontrolled search path vulnerability on Windows that can lead to arbitrary code execution when converting Jupyter notebooks containing SVG output to PDF format. The vulnerability allows an attacker to place a malicious `inkscape.bat` file in the working directory.

#### Impact on stacks-data Users

- **Linux/macOS:** Not affected (vulnerability is Windows-specific)
- **Windows:** Affected only when using the `data-quality` extra with Great Expectations and performing notebook-to-PDF conversions

#### Mitigations

Since no patched version is available, we recommend the following mitigations for Windows users:

1. **Avoid PDF Conversions (Recommended)**
   - Do not use `jupyter nbconvert --to pdf` on Windows systems
   - Use alternative formats (HTML, Markdown) for notebook exports
   - Use Linux-based CI/CD pipelines for any PDF generation

2. **Working Directory Security**
   - Never run nbconvert in untrusted directories on Windows
   - Scan working directories for suspicious `.bat` files before conversion
   - Use absolute paths when specifying executables

3. **Alternative Installation**
   - Consider installing inkscape from the official distribution and adding it to PATH
   - This reduces reliance on search path resolution

4. **Environment Isolation**
   - Run notebook conversions in isolated containers or VMs on Windows
   - Use WSL2 (Windows Subsystem for Linux) for notebook operations

5. **Monitoring**
   - Implement file system monitoring for unexpected `.bat` file creation
   - Review process execution logs for suspicious inkscape invocations

#### For CI/CD Pipelines

If your CI/CD pipeline runs on Windows and uses the data-quality features:

```yaml
# Recommended: Use Linux-based runners
jobs:
  test:
    runs-on: ubuntu-latest # Not windows-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install dependencies
        run: pip install stacks-data[data-quality]
```

#### Tracking

- **GitHub Advisory:** [GHSA-xm59-rqc7-hhvf](https://github.com/advisories/GHSA-xm59-rqc7-hhvf)
- **CVE:** CVE-2025-53000
- **Dependabot Alert:** [#50](https://github.com/Ensono/stacks-data/security/dependabot/50)

We will update this document when a patched version of nbconvert becomes available.

## Security Best Practices

### General Recommendations

1. **Keep Dependencies Updated**
   - Regularly run `poetry update` to get security patches
   - Monitor Dependabot alerts in the GitHub repository

2. **Use Minimal Installation**
   - Only install extras you need: `pip install stacks-data` (core only)
   - Avoid installing `data-quality` extra on Windows if PDF conversion is required

3. **Azure Credentials**
   - Use Azure Managed Identities where possible
   - Never commit credentials to version control
   - Use environment variables or Azure Key Vault for secrets

4. **Code Signing**
   - All commits must be GPG signed (see [Security Instructions](.github/copilot-security-instructions.md))
   - Follow the contribution guidelines for security compliance

5. **Spark Security**
   - Use encrypted connections for Spark clusters
   - Enable Spark authentication when running in multi-tenant environments
   - Follow Azure Data Lake access control best practices

## Compliance

This project follows security standards including:

- ISO 27001:2013
- NIST SP 800-53
- OWASP Top 10
- CIS Controls

See [Security Instructions](.github/copilot-security-instructions.md) for detailed requirements.

## License

Security policy is licensed under the same terms as the main project (see LICENSE).
