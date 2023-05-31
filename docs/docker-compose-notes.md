#### Notes:


Arborist has a usr table, full of users synced from user.yaml.  It also has tables usr_grp, grp and grp_policy all populated from the user.yaml sync.    Arborist also has apis to add users, groups etc.   They are not exposed outside of the cluster.  This is probably a good idea.  Appropriately, it is up to something within the cluster to access those APIs, in this case Requestor.

Requestor will read policies and permissions from arborist regarding a users ability to request and grant access by user. e.g: [user.yaml](https://ohsuitg-my.sharepoint.com/:u:/r/personal/walsbr_ohsu_edu/Documents/[aced-staging-requestor-05-11-2023.yaml](https://ohsuitg-my.sharepoint.com/:u:/r/personal/walsbr_ohsu_edu/Documents/aced-staging-requestor-05-11-2023.yaml?csf=1&web=1&e=LhW55g)?csf=1&web=1&e=LhW55g)

docker-compose snippets:

```
  requestor-service:
    build: requestor
    # image: "quay.io/cdis/requestor"
    container_name: requestor-service
    networks:
      - devnet
    volumes:
      - ./Secrets/requestor-config.yaml:/src/requestor-config.yaml
    environment:
      - DB_DATABASE=requestor_db
      - DB_USER=XXXXX
      - DB_PASSWORD=XXXXX
      - DB_HOST=postgres
      - DB_PORT=5432
      - PGSSLMODE=disable
      - GEN3_ARBORIST_ENDPOINT=http://arborist-service
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost/health"]
      interval: 60s
      timeout: 5s
      retries: 10

    # note: requestor's auth client get's the "iss" claim of the JWT token and will connect and validate against that url.
    # therefore, if that endpoint is not a public DNS entry, the requestor's container needs to resolve that host
    # eg.
    extra_hosts:
     - "aced-training.compbio.ohsu.edu:THE-IP-ADDRESS-OF-THE-HOST-OS"

```

revproxy-service nginx snippet:
```
      location /requestor/ {
          rewrite ^/requestor/(.*) /$1 break;
          auth_request_set $saved_set_cookie $upstream_http_set_cookie;
          proxy_pass http://requestor-service/;
            proxy_http_version 1.1;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $http_connection;
            proxy_set_header Authorization "$access_token";
            client_max_body_size 0;
      }
```


postgres setup:

In `scripts/postgres_init.sql`

```
CREATE DATABASE arborist_db;
CREATE USER requestor_user;
ALTER USER requestor_user WITH PASSWORD 'XXXXX';
ALTER USER requestor_user WITH SUPERUSER;
```

> helm
The [helm chart](https://github.com/uc-cdis/gen3-helm/tree/master/helm/requestor) seems complete.  We have yet to test.


See more:

https://github.com/uc-cdis/requestor/blob/master/docs/authorization.md
