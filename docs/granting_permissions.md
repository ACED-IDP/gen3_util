# Permissions Use Case

The below is a full example use case where --profile staging is the admin profile's 
command line actions and --profile staging-low can be the end user's command line actions.

The the g3t utilities access commands can be requested and approved by the admin/data steward as well.

The end user is expected to know how to do init, add, meta create, commit and push commands.


#### The admin or data steward with sheepdog-admin permissions creates the project resource
```
g3t --profile staging utilities projects create /programs/{program}/projects/{project}
```

#### The end user initializes the project in their project directory
```
g3t --profile staging-low init {program}-{project}
```

#### A reader role request on program-project is created
```
g3t --profile staging-low utilities access add {email} --resource_path /programs/{program}/projects/{project} --roles reader
```
#### A writer role request on program-project is created
```
g3t --profile staging-low utilities access add {email} --resource_path /programs/{program}/projects/{project} --roles writer
```

#### A deleter role request on program-project is created
```
g3t --profile staging-low utilities access add {email} --resource_path /programs/{program}/projects/{project} --roles deleter
```

#### All of the above requests are signed by a data steward / admin.
```
g3t --profile staging utilities access sign
```

#### The end user adds the relevant files in their local directory that they want to upload to a gen3 bucket.
```
g3t add ....
```

#### The end user generates metadata that correlates with added files or attaches their own valid fhir metadata in the META directory
```
g3t utilities meta create
```

#### The end user runs the commit command which also checks to see if meta data is valid or not.
```
g3t --profile staging-low commit -m "TEST"
```

#### The end user pushes their files and metadata to gen3 via a job
```
g3t --profile staging-low push
```

#### Upon successful job completion, project should be populated in the exporation page and front summary bar chart, but the discovery page should not have changed

#### If the end user wishes to completely empty all data from a project they can run the below command
```
g3t --profile staging-low reset
```
#### If an admin or data steward wishes to remove an emptied project from the gen3 site entirely they can run the below command
```
g3t --profile staging rm --project_id {program}-{project}
```
#### There should be no evidence that the project existed on the site
