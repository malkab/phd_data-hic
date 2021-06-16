# Project Description

This is the template folder for a data science project in GIT.


## Big Data Projects Patterns

In very big, modular projects, it is wise to subdivide the project in several sub repos in a GitLab group. Each repo will handle a set of data, while some repos (usually one) will provide the cohesive Docker infrastructure for products and interoperation. The sub repos may have their own Docker infrastructure, although most of the times this won't be necessary.


## Big Data: Set DVC

Use DVC for storing big data assets. The pattern is to be very atomic regarding data assets, follow this guidelines:

- as a general rule, don't add to DVC whole folders like **data**, **000_in**, or **900_out**. Be more selective, specially in the case of very large files. Add them individually;

- folders that sum up a set of data that works together may be added as a folder.

This allows for a more atomic retrieval of data when needed.

When deciding the remote, using a common remote for several repos has the advantage of reducing remote size if those repos share data, but it's detrimental toward flexibility of moving and storing data for idle repos in other storage media. So, when a set of repos will share a big deal of data (for example, PhD), use the same remote for all repos, and use different ones in all the other cases.

Follow these steps:

```Shell
# Initialise Git and DVC.
# Will generate some files that are directly added to the stage zone for the
# next commit.
git init
git flow init
dvc init

# Add a remote for DVC, multiple remote types are available.
# Remotes are added to .dvc/config. Also configure it for use as local cache so
# it doesn't use additional space.
dvc remote add -d storage /mnt/samsung_hdd_1_5tb/dvc_storage/project_name_if_applicable
dvc config cache.local storage

# Add data for tracking with DVC.
# This will hash the files and create the cache. BEWARE!!! THE CACHE CONSUMES
# SO MUCH SPACE AS THE ORIGINAL FILES IF NOT USED IN cache.local MODE (see
# below). The cache is at .dvc/cache.
# dvc tracked content will be automatically added to the upper folder level
# .gitignore.
# For each folder or file added, a .dvc file is created and that needs to be
# commited.
dvc status
dvc add data

# Push
dvc push
git push

# Once the repo is cloned, a pull will retrieve the big data if the remote is
# available.
git pull
dvc pull
```


## Dropping DVC-Tracked Data

DVC-tracked data can be safely dropped. **dvc status** will label them as **deleted**, but can't be commited.
