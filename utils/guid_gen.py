import uuid

NAMESPACE_OWNER  = uuid.uuid5(uuid.NAMESPACE_DNS, "github.owner")
NAMESPACE_REPO   = uuid.uuid5(uuid.NAMESPACE_DNS, "github.repo")
NAMESPACE_ISSUE  = uuid.uuid5(uuid.NAMESPACE_DNS, "github.issue")
NAMESPACE_BRANCH = uuid.uuid5(uuid.NAMESPACE_DNS, "github.branch")

def generate_guid(namespace: uuid.UUID, key: str) -> str:
    return str(uuid.uuid5(namespace, key))