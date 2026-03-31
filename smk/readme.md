# snakemake workflow

## use

```bash
#set the profile
export SNAKEMAKE_PROFILE=smk/profiles






# r01_prep -----------------------------
# single-tile local prep proof
snakemake --force workflow_outdir/01_prep/FLUVIAL/UNDEFENDED/1in1000/n51w115/r01_prep.tif

# all filtered prep invocations
snakemake -n r01_prep_all





# r02_hrdem -----------------------------
# single-tile local HRDEM proof
snakemake workflow_outdir/02_hrdem/PLUVIAL/DEFENDED/1in1000/n49w124/r02_hrdem.vrt

# all filtered HRDEM invocations
snakemake -n r02_hrdem_all


# r03_tohr -----------------------------
snakemake workflow_outdir/03_tohr/FLUVIAL/UNDEFENDED/1in1000/n51w115/r03_tohr.vrt

snakemake r03_tohr_all
```
