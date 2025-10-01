# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

If you discover a security vulnerability in QuantMini, please report it to us privately:

### How to Report

1. **Email**: Send details to zheyuan28@gmail.com
2. **Subject**: Include "SECURITY" in the subject line
3. **Include**:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

### What to Expect

- **Acknowledgment**: Within 48 hours
- **Initial Assessment**: Within 7 days
- **Updates**: Regular updates on the progress
- **Resolution**: We aim to release a fix within 30 days for critical issues

### Disclosure Policy

- We will coordinate disclosure with you
- Security advisories will be published after fixes are released
- You will be credited (if desired) in the security advisory

## Security Best Practices

### For Users

**1. Credentials Management**
- Never commit `config/credentials.yaml` to version control
- Use environment variables for production deployments
- Rotate API keys regularly
- Use AWS Secrets Manager or similar for cloud deployments

**2. Data Security**
- Never commit data files to version control
- Ensure `data/` directory is in `.gitignore`
- Use encryption for sensitive data at rest
- Implement access controls on data directories

**3. Dependencies**
- Keep dependencies up to date: `uv pip list --outdated`
- Review dependency security advisories
- Use virtual environments to isolate dependencies

**4. Network Security**
- Use HTTPS for all API communications
- Validate SSL certificates
- Use VPN when accessing sensitive data sources
- Implement rate limiting for API calls

### For Contributors

**1. Code Review**
- All code changes must be reviewed before merging
- Check for exposed secrets in commits
- Validate input data before processing
- Use parameterized queries to prevent injection

**2. Testing**
- Write tests for security-critical code
- Test error handling and edge cases
- Validate authentication and authorization

**3. Documentation**
- Document security considerations for new features
- Update security policy when adding sensitive features
- Provide secure configuration examples

## Known Security Considerations

### API Keys
- Polygon.io API keys provide access to your subscription
- Store keys securely and never expose in logs
- Use read-only keys when possible

### AWS Credentials
- S3 access requires AWS credentials
- Use IAM roles with least privilege
- Enable MFA for AWS accounts

### Data Privacy
- Market data may be proprietary and have usage restrictions
- Ensure compliance with data provider terms of service
- Implement appropriate access controls

## Vulnerability Types We're Interested In

- Remote code execution
- SQL injection or similar data injection
- Authentication/authorization bypass
- Information disclosure (credentials, keys, sensitive data)
- Denial of service
- Insecure dependencies

## Out of Scope

The following are generally out of scope:
- Social engineering attacks
- Physical security
- Attacks requiring physical access
- Issues in dependencies (report to the dependency maintainers)

## Security Updates

Security updates will be released as:
- **Critical**: Immediate patch release
- **High**: Patch within 7 days
- **Medium**: Included in next regular release
- **Low**: Documented and scheduled for future release

## Contact

For security issues: zheyuan28@gmail.com

For general issues: https://github.com/nittygritty-zzy/quantmini/issues

---

**Note**: This security policy is subject to change. Please check back regularly for updates.
