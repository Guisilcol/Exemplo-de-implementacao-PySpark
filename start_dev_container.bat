docker run -it \
    -v DIRETORIO_AWS:/home/glue_user/.aws \
    -v ./:/home/glue_user/workspace/ \
    -v AWS_PROFILE=default \
    -e DISABLE_SSL=true --rm \
    -e DATALAKE_FORMATS=iceberg \
    -p 4040:4040 \
    -p 18080:18080 \
    --name glue_pyspark \
    amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
    pyspark