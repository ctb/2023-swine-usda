# TODO:
# - make k configurable
# - make scaled configurable

INPUT_DIR='sigs'
OUTPUT_DIR='outputs.swine-x-reps'
DB='/group/ctbrowngrp/sourmash-db/gtdb-rs214/gtdb-rs214-reps.k21.zip'
DBLIST='list.gtdb-reps-rs214-k21.txt'

ACCESSIONS, = glob_wildcards(f"{INPUT_DIR}/{{acc}}.sig")

rule all:
    input:
        expand(f"{OUTPUT_DIR}/{{acc}}.gather.out", acc=ACCESSIONS)
    

rule fastgather:
    input:
#        wort="/group/ctbrowngrp/irber/data/wort-data/wort-sra/sigs/{acc}.sig",
        wort=f"{INPUT_DIR}/{{acc}}.sig",
        against=DBLIST,
    output:
        csv=f"{OUTPUT_DIR}/{{acc}}.fastgather.csv"
    shell: """
        sourmash scripts fastgather {input.wort} {input.against} \
            -o {output.csv} -k 21 --scaled 10000
    """

rule gather:
    input:
#        wort="/group/ctbrowngrp/irber/data/wort-data/wort-sra/sigs/{acc}.sig",
        wort=f"{INPUT_DIR}/{{acc}}.sig",
        db=DB,
        picklist=f"{OUTPUT_DIR}/{{acc}}.fastgather.csv"
    output:
        out=f"{OUTPUT_DIR}/{{acc}}.gather.out",
        csv=f"{OUTPUT_DIR}/{{acc}}.gather.csv",
    shell: """
        sourmash gather -k 21 --scaled 10000 {input.wort} {input.db} \
           --picklist {input.picklist}:match:ident -o {output.csv} >& {output.out}
    """
