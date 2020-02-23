# About

- This project uses github actions for CI/CD. Read below how to setup CI/CI initially and for a new branch

At a high-level github actions work by having a workflow file in `.github/workflows`. For example, we use
`.github/workflows/main.yml`. In the yml file there is an instruction called `uses`. For example,
`uses: MLStream/mlstream-spark-udfs/actions@actions_mytag`. `actions_mytag` is a release.
The release `actions_mytag` can be created from any branch, but the release needs to exist 
so that we can run CI.

# Short story

1. Create a new branch
2. Update `Dockerfile`, `actions/`, `spark/build.sbt` as necessary.
3. Create a new release
4. Edit the workflow file to use the release. See below "Updating the workflow in your original file"
5. Check that the builds succeeds
6. If you had to make modification to the source code you may want to make a new release

Unfortunately, this is circular, but is the easiest way. 


# Creating a new release with the actions code

- Go go releases
- Click 'Draft a new release'
- Enter `actions_mytag@mybranch`
- Enter release title "This release is needed only for github to build a test image for CI"
- Click this is a pre-release
- Click "Publish"

# Updating the workflow in your original file

- Go to the branch with the source code (e.g. mybranch)
- In mybranch, go to `.github/workflows/main.yml`
- Update the uses instruction with `uses: MLStream/mlstream-spark-udfs/actions@actions_mytag`
- Observe that mybranch builds correctly 

