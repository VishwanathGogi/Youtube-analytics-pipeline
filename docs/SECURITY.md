# Security and credential handling

## Authentication model

This project uses workload identity rather than embedded credentials.

- **Lambda** receives temporary AWS credentials from its execution role.
- **AWS Glue** receives temporary AWS credentials from its job role.
- AWS SDK clients discover and rotate those credentials through the runtime credential provider chain.
- Requests to S3 and Glue are signed by the AWS SDK. Application code does not manually create or persist tokens.

Do not place access-key IDs, secret keys, session tokens, passwords, or API keys in this repository or in Lambda environment variables.

## Recommended least-privilege access

The Lambda role should be restricted to:

- `s3:GetObject` on the category JSON input prefix
- `s3:PutObject` on the category cleansed prefix
- the minimum Glue database/table permissions required by AWS SDK for pandas
- CloudWatch Logs permissions for its own log group

The Glue role should be restricted to:

- read access to the raw statistics prefix
- write access to the cleansed statistics prefix
- access to the relevant Glue Catalog databases and tables
- CloudWatch logging for its own job

Use separate roles for Lambda and Glue. Restrict resources to exact bucket prefixes and catalog objects instead of `*` wherever AWS supports it.

## Data protection

- Enable S3 Block Public Access.
- Encrypt S3 buckets with SSE-KMS when organizational policy requires customer-managed keys.
- Require TLS for bucket access.
- Restrict KMS key policies to the execution roles that need them.
- Set CloudWatch retention deliberately.
- Avoid logging record contents that may contain sensitive information.

## Event validation

The Lambda validates the S3 event shape, accepts JSON objects only, and can restrict processing to an expected source bucket through `ALLOWED_SOURCE_BUCKET`.

IAM and bucket policies remain the primary authorization boundary; code validation is defense in depth.

## Secret scanning

Enable GitHub secret scanning and push protection where available. If a credential is ever committed, remove it from use immediately and rotate or revoke it; deleting the line from a later commit is insufficient.
