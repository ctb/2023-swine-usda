# TODO:
# - make k configurable
# - make scaled configurable
# WORT sigs copied from:
#   "/group/ctbrowngrp/irber/data/wort-data/wort-sra/sigs/{acc}.sig",

if 0:
    INPUT_DIR='sigs'
    OUTPUT_DIR='outputs.swine-x-reps'
    DB='/group/ctbrowngrp/sourmash-db/gtdb-rs214/gtdb-rs214-reps.k21.zip'
    DBLIST='list.gtdb-reps-rs214-k21.txt'
else:
    INPUT_DIR='sigs'
    OUTPUT_DIR='outputs.swine-x-all'
    DB='/group/ctbrowngrp/sourmash-db/gtdb-rs214/gtdb-rs214-k21.zip'
    DBLIST='list.gtdb-rs214-k21.txt'

ACCESSIONS, = glob_wildcards(f"{INPUT_DIR}/{{acc}}.sig")

rule all:
    input:
        expand(f"{OUTPUT_DIR}/{{acc}}.gathertax.human.txt", acc=ACCESSIONS)
    

rule fastgather:
    input:
        wort=f"{INPUT_DIR}/{{acc}}.sig",
        against=DBLIST,
    output:
        csv=touch(f"{OUTPUT_DIR}/{{acc}}.fastgather.csv")
    resources:
        # limit to one fastgather with --resources rayon_exclude=1
        rayon_exclude=1
    shell: """
        sourmash scripts fastgather {input.wort} {input.against} \
            -o {output.csv} -k 21 --scaled 10000
    """

rule subtract_host:
    input:
        wort=f"{INPUT_DIR}/{{acc}}.sig",
        host="hg38+susScr11.sig.gz",
    output:
        out=f"{OUTPUT_DIR}/{{acc}}.subtract.sig.gz"
    shell: """
        sourmash sig subtract -A {input.wort} {input.wort} {input.host} \
            -o {output} -k 21
    """

rule gather:
    input:
        sig=f"{OUTPUT_DIR}/{{acc}}.subtract.sig.gz",
        db=DB,
        picklist=f"{OUTPUT_DIR}/{{acc}}.fastgather.csv"
    output:
        out=f"{OUTPUT_DIR}/{{acc}}.gather.out",
        csv=f"{OUTPUT_DIR}/{{acc}}.gather.csv",
    shell: """
        sourmash gather -k 21 --scaled 10000 {input.sig} {input.db} \
           --picklist {input.picklist}:match:ident -o {output.csv} >& {output.out}
    """

rule tax:
    input:
        csv=f"{OUTPUT_DIR}/{{acc}}.gather.csv",
        taxdb="gtdb-rs214.lineages.sqldb",
    output:
        csv=f"{OUTPUT_DIR}/{{acc}}.gathertax.summarized.csv",
        human=f"{OUTPUT_DIR}/{{acc}}.gathertax.human.txt",
    params:
       prefix=f"{OUTPUT_DIR}/{{acc}}.gathertax"
    shell: """
        sourmash tax metagenome -t {input.taxdb} -g {input.csv} \
             -o {params.prefix} -F csv_summary
        sourmash tax metagenome -t {input.taxdb} -g {input.csv} \
             -o {params.prefix} -F human
    """
