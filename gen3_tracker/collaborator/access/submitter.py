
from gen3.auth import Gen3Auth

from gen3_tracker import Config
from gen3_tracker.config import ensure_auth


def ensure_program_project(config: Config, project_id: str, auth: Gen3Auth = None) -> str:
    """Ensure program and project exist in sheepdog.
    """
    # improve startup time by importing only what is needed
    from gen3.submission import Gen3Submission

    if not auth:
        auth = ensure_auth(config=config)
    program, project = project_id.split('-')
    submission = Gen3Submission(auth)
    msgs = []
    programs = [_.split('/')[-1] for _ in submission.get_programs()['links']]
    if program not in programs:
        submission.create_program({'name': program, 'type': 'program', 'dbgap_accession_number': program})
        msgs.append(f"Created program: {program}")
    projects = [_.split('/')[-1] for _ in submission.get_projects(program)['links']]
    if project not in projects:
        submission.create_project(program, {'code': project, 'type': 'project', "state": "open", "dbgap_accession_number": project})
        msgs.append(f"Created project: {project}")
    return ', '.join(msgs)
