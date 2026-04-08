submit_all_batches.sh

└─ sbatch tcga_batch_5samples.sh uuid1 uuid2 ... uuid5


     └─ PipeA (gdc_download_pipeA.sh) — runs once, exits when all UUIDs downloaded

     
     └─ PipeB (downsample_pipeB.sh) — poll loop, wakes up as PipeA .done markers appear. Downsamples bams to 10X coverage, then deletes full bams

     
     └─ PipeC (ampsuite_pipeC.sh) — poll loop, wakes up as PipeB .done markers appear. Runs AA and AC on downsampled bams
