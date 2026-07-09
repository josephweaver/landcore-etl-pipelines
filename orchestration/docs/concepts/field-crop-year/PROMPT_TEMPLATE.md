# Reusable Implementation Prompt Template

Use this template for each OS implementation.

```text
Please implement the LandCore operational slice:

<OS_PATH>

This is LandCore repository work only.

Do not modify GORC core, GORC worker, GORC controller, GORC data-asset providers, or GORC geospatial plugins.

Allowed paths are limited to the LandCore repository, especially:

orchestration/**

Your task:
- Read the OS.
- Implement only the requested slice.
- Keep the change small and operational.
- Prefer standard-library Python unless the OS explicitly allows another dependency.
- Use project.json, workflow.json, submission JSON, config JSON, Python scripts, runbooks, and smoke scripts.
- Preserve the GORC ownership boundary.
- If current GORC behavior blocks the OS, stop and append a concise blocker to orchestration/docs/issues.md. Do not patch GORC.

When complete:
1. Run the validation commands listed in the OS, or state exactly why they could not be run.
2. Update orchestration/docs/STATE.md.
3. Mark the OS as implemented in the OS file.
4. Add a short usage note to orchestration/docs/usage/<yyyyMMddHHmmss>-<os-id>.md using the local usage template if present.
```
