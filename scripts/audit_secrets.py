"""Read-only local/Git audit. Emit locations and rule names, never secret values."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess

from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parents[1]
SKIP = {'.git', '.venv', '.uv-cache', '__pycache__', '.pytest_cache', 'node_modules', 'build', 'dist'}
RULES = {
    'private_key': rb'-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----',
    'aws_access_key': rb'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b',
    'github_token': rb'\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})\b',
    'slack_token': rb'\bxox[baprs]-[A-Za-z0-9-]{20,}\b',
    'openai_key': rb'\bsk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{32,}\b',
    'credential_url': rb'(?i)\b(?:https?|postgres(?:ql)?|mysql)://[^\s/:]+:[^\s/@]+@',
    'jwt': rb'\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b',
}
ASSIGNMENT = re.compile(rb'''(?im)["']?\b([A-Z_]*(?:PASSWORD|SECRET|TOKEN|API_KEY|ACCESS_KEY)[A-Z_]*)["']?[ \t]*(?:=|:)[ \t]*["']?([^\s"',;}#]+)''')


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    env = dotenv_values(ROOT / '.env')
    secrets = {k: v.encode() for k, v in env.items() if v and re.search('PASSWORD|SECRET|TOKEN|API_KEY', k)}
    identifiers = {k: env[k].encode() for k in ('SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER') if env.get(k)}
    compiled = {name: re.compile(pattern) for name, pattern in RULES.items()}
    findings, assignments, metadata = [], [], []

    def inspect(data, location):
        for key, value in secrets.items():
            if value in data:
                findings.append({'location': location, 'rule': 'exact_local_secret', 'variable': key})
        for name, rule in compiled.items():
            if rule.search(data):
                findings.append({'location': location, 'rule': name})
        for match in ASSIGNMENT.finditer(data):
            value = match.group(2)
            if value.startswith((b'$', b'<')) or value in (b'None', b'True', b'False', b'SecretStr(', b'Field('):
                continue
            assignments.append({'location': location, 'rule': 'credential_assignment_candidate',
                                'line': data[:match.start()].count(b'\n') + 1})
        for key, value in identifiers.items():
            if value in data:
                metadata.append({'location': location, 'rule': 'local_account_metadata', 'variable': key})

    file_count = 0
    for directory, dirs, files in os.walk(ROOT):
        dirs[:] = [name for name in dirs if name not in SKIP]
        for name in files:
            path = Path(directory, name)
            if path.is_symlink():
                continue
            inspect(path.read_bytes(), 'file:' + str(path.relative_to(ROOT)))
            file_count += 1
    # --batch-all-objects includes unreachable loose/packed objects, not only HEAD.
    objects = git('cat-file', '--batch-all-objects', '--batch-check=%(objectname) %(objecttype)').decode().splitlines()
    counts = {}
    for entry in objects:
        oid, kind = entry.split()
        counts[kind] = counts.get(kind, 0) + 1
        if kind in ('blob', 'commit', 'tag'):
            inspect(git('cat-file', kind, oid), 'git:' + kind + ':' + oid)
    for name in ('config',):
        path = Path(git('rev-parse', '--git-path', name).decode().strip())
        if not path.is_absolute():
            path = ROOT / path
        if path.exists():
            inspect(path.read_bytes(), 'git-local:' + name)
    report = {'scope': {'workspace_files': file_count, 'git_objects': counts, 'ignored_directories': sorted(SKIP),
                        'git_is_shallow': git('rev-parse', '--is-shallow-repository').decode().strip() == 'true',
                        'exact_secret_variables': sorted(secrets)},
              'findings': findings, 'assignment_candidates': assignments, 'account_metadata': metadata,
              'limitations': 'Heuristics plus exact current local secrets; no guarantee for unknown, old, encoded or external secrets. Values are never reported.'}
    output = json.dumps(report, indent=2) + '\n'
    if args.output:
        args.output.write_text(output)
    print(json.dumps({'files_checked': file_count, 'git_objects': counts, 'findings': len(findings),
                      'assignment_candidates': len(assignments), 'account_metadata_locations': len(metadata)}))


if __name__ == '__main__':
    main()
