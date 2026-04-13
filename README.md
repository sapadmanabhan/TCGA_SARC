The _watcher.sh scripts are lightweight pollers that run 24/7 on genomemaster and never hold a SLURM slot. The _script.sh files are the actual workers that only occupy resources while actively running.

PipeA_watcher.sh -->  Polls manifest, enforces ≤5 cached BAMs, calls PipeA_script.sh directly in background

PipeA_script.sh -->  Downloads + validates one UUID; writes .done marker

PipeB_watcher.sh --> Polls PipeA .done/; calls sbatch PipeB_script.sh per UUID

PipeB_script.sh --> Indexes + downsamples one BAM; deletes full BAM on success on Slurm

PipeC_watcher.sh --> Polls PipeB .done/; submits sbatch PipeC_script.sh (or runs locally if --genomemaster flag set after checking with Jens)

PipeC_script.sh     --> Runs AmpliconSuite AA+AC for one sample on Slurm

