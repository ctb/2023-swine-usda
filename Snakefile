# TODO:
# - make k configurable
# - make scaled configurable
# WORT sigs copied from:
#   "/group/ctbrowngrp/irber/data/wort-data/wort-sra/sigs/{acc}.sig",

if 1:                           # swine x reps
    INPUT_DIR='sigs'
    OUTPUT_DIR='outputs.swine-x-reps'
    DB='/group/ctbrowngrp/sourmash-db/gtdb-rs214/gtdb-rs214-reps.k21.zip'
    DBLIST='list.gtdb-reps-rs214-k21.txt'
elif 0:                         # swine x all
    INPUT_DIR='sigs'
    OUTPUT_DIR='outputs.swine-x-all'
    DB='/group/ctbrowngrp/sourmash-db/gtdb-rs214/gtdb-rs214-k21.zip'
    DBLIST='list.gtdb-rs214-k21.txt'
elif 0:                         # human.x.all
    INPUT_DIR='sigs.human'
    OUTPUT_DIR='outputs.human-x-all'
    DB='/group/ctbrowngrp/sourmash-db/gtdb-rs214/gtdb-rs214-k21.zip'
    DBLIST='list.gtdb-rs214-k21.txt'
elif 0:                         # human.x.reps
    INPUT_DIR='sigs.human'
    OUTPUT_DIR='outputs.human-x-reps'
    DB='/group/ctbrowngrp/sourmash-db/gtdb-rs214/gtdb-rs214-reps.k21.zip'
    DBLIST='list.gtdb-reps-rs214-k21.txt'

ACCESSIONS, = glob_wildcards(f"{INPUT_DIR}/{{acc}}.sig")

rule all:
    input:
        expand(f"{OUTPUT_DIR}/{{acc}}.gathertax.human.txt", acc=ACCESSIONS),
        expand(f"{OUTPUT_DIR}/{{acc}}.x.host.prefetch.csv", acc=ACCESSIONS)

# test!
rule fastmultigather:
    input:
        queryfile="list.human.sigs.txt",
        #against="list.host+gtdb-rs214-k21.txt",
        against="/home/ctbrown/scratch/2022-database-covers/list.gtdb-rs214.k21.cover.txt",
    threads: 128
    shell: """
        /usr/bin/time -v sourmash scripts fastmultigather {input.queryfile} {input.against} \
            -k 21 -c {threads} -s 10000
    """
    
rule fastmultigather_all:
    input:
        queryfile="list.all-swine-sra.txt",
        #against="list.host+gtdb-rs214-k21.txt",
        #against="/home/ctbrown/scratch/2022-database-covers/list.gtdb-rs214.k21.cover.txt",
        against="list.host+gtdb-rs214-k21-cover.txt",
    threads: 128
    shell: """
        /usr/bin/time -v sourmash scripts fastmultigather {input.queryfile} {input.against} \
            -k 21 -c {threads} -s 10000
    """

rule fastgather:
    input:
        wort=f"{INPUT_DIR}/{{acc}}.sig",
        against=DBLIST,
    output:
        csv=touch(f"{OUTPUT_DIR}/{{acc}}.fastgather.csv")
    resources:
        # limit to one fastgather with --resources rayon_exclude=1
        rayon_exclude=1
    threads: 32
    shell: """
        sourmash scripts fastgather {input.wort} {input.against} \
            -o {output.csv} -k 21 --scaled 10000 -c {threads}
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

rule containment:
    input:
        wort=f"{INPUT_DIR}/{{acc}}.sig",
        host="hg38+susScr11.sig.gz",
    output:
        csv=f"{OUTPUT_DIR}/{{acc}}.x.host.prefetch.csv",
        csv2=f"{OUTPUT_DIR}/{{acc}}.x.host.search.csv",
        out=f"{OUTPUT_DIR}/{{acc}}.x.host.search.out"
    shell: """
        sourmash prefetch {input.wort} {input.host} -k 21 -o {output.csv}
        sourmash search --containment {input.host} {input.wort} -k 21 -o {output.csv2}  > {output.out}
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
