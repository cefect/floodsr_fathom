# gen
- when proving/running tests, use a timeout of 30 secs
- for tests that dont complete, include a single bash command to run those


## snakemake (smk)
- prove/develop on a single tile tiny tile
- full run is only proven on two full tiles (for each dimension)
- point snakemake runs using the SNAKEMAKE_PROFILE env var (i.e, `export SNAKEMAKE_PROFILE=smk/profiles`). do not call the snakefile directly. 
- include comments for every major flow code block for readability (usually a single line)
