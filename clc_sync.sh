####################### Import to CLC here #######################
    SERVER=medair.sahlgrenska.gu.se
    PORT=7777
    USER=cmduser
    PASS=$(cat /home/xcanfv/clc_passwords.txt | grep cmduser | cut -f 2)
    CLCSERVER=$(echo "/apps/clcservercmdline/clcserver -S $SERVER -P $PORT -U ${USER} -W ${PASS}")
    RUN=$1
    SAVELOC=/medstore/results/clinical/SARS-CoV-2-typing/nextseq_data/$RUN/fasta/
    echo $SAVELOC
    CLC_FASTA=$(find $SAVELOC -name "*.fa")
    RUNSTDOUT=/medstore/logs/pipeline_logfiles/sars-cov-2-typing/nextseq_clcimport.log

    # Create directory
    $CLCSERVER -A mkdir -t "clc://server/CLC_Data_Folders/Microbiology/SARS-CoV-2_Clinical/Illumina/" -n $RUN
    # Import the fasta files
    for value in $CLC_FASTA 
    do
        $CLCSERVER -G clinical-production -A import -f fasta -s clc://serverfile/$value -d clc://server/CLC_Data_Folders/Microbiology/SARS-CoV-2_Clinical/Illumina/$RUN >> $RUNSTDOUT 2>&1
    done

    #checkExit $? "imported csv into CLC" >> $RUNSTDOUT

