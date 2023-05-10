To create large files:

```
cat /dev/urandom | head -c  `echo "1073741824*1" | bc`  > DATA/file-random-contents-1GB.txt
cat /dev/urandom | head -c  `echo "1073741824*2" | bc`  > DATA/file-random-contents-2GB.txt
cat /dev/urandom | head -c  `echo "1073741824*3" | bc`  > DATA/file-random-contents-3GB.txt
cat /dev/urandom | head -c  `echo "1073741824*4" | bc`  > DATA/file-random-contents-4GB.txt

$tree DATA
DATA
├── file-random-contents-1GB.txt
├── file-random-contents-2GB.txt
├── file-random-contents-3GB.txt
└── file-random-contents-4GB.txt


```

To create meta (FHIR):

```
$gen3_util meta import dir DATA/ FHIR/ --project_id aced-ohsu_download_testing
msg: OK
summary:
  DocumentReference:
    count: 4
    size: 10737418240
  ResearchStudy:
    count: 1

$tree FHIR
FHIR
├── DocumentReference.ndjson
└── ResearchStudy.ndjson

```

Optional, validate meta data:

```
$ gen3_util meta validate FHIR
exceptions: []
msg: OK
resources:
  summary:
    DocumentReference: 4
    ResearchStudy: 1
```

Verify the project exists:

```
$ gen3_util projects ls
endpoint: https://staging.aced-idp.org
msg: OK
projects:
- /programs/aced/projects/Alcoholism
- /programs/aced/projects/Alzheimers
- /programs/aced/projects/Breast_Cancer
- /programs/aced/projects/Colon_Cancer
- /programs/aced/projects/Diabetes
- /programs/aced/projects/Lung_Cancer
- /programs/aced/projects/Prostate_Cancer
- /programs/aced/projects/ohsu_download_testing

```

Verify the bucket exists:

```
$ gen3_util buckets  ls
buckets:
  GS_BUCKETS: {}
  S3_BUCKETS:
    aced-default:
      endpoint_url: https://minio-default-staging.aced-idp.org
      region: us-east-1
    aced-manchester:
      endpoint_url: https://minio-manchester-staging.aced-idp.org
      region: us-east-1
    aced-ohsu-staging:
      endpoint_url: https://aced-storage.ohsu.edu/
      region: us-east-1
    aced-stanford:
      endpoint_url: https://minio-stanford-staging.aced-idp.org
      region: us-east-1
    aced-ucl:
      endpoint_url: https://minio-ucl-staging.aced-idp.org
      region: us-east-1
endpoint: https://staging.aced-idp.org
msg: OK

```

Copy files to the bucket:

```

gen3_util files cp --project_id aced-ohsu_download_testing FHIR/DocumentReference.ndjson  bucket://aced-ohsu-staging

100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 10.0G/10.0G [29:06<00:00, 6.15MB/s, elapsed=0:29:05.299469, file=0ca1b6]
errors: []
incomplete: []
info:
- Wrote state to ~/.gen3/gen3-util-state/state.ndjson
msg: OK

```
