import os
import json
import click
import boto3

@click.command()
@click.argument("letter")
def cli(letter):

    data = list()
    part = 0
    bucket = "gfw-files"
    s3_folder = "2018_update/results/20190425/"
    datasets = ["iso", "adm1", "adm2"]
    for dataset in datasets:

        prefix = os.path.join(s3_folder, letter, dataset)
        click.echo("Searching: " + prefix)

        for obj in _get_s3_records(bucket, prefix):

            filename, file_extension = os.path.splitext(obj.key)
            if file_extension == ".txt":
                click.echo("Download: " + os.path.basename(obj.key))
                data += json.loads(obj.get()['Body'].read().decode('utf-8'))
                if len(data) > 50000:
                    file_name = '{}-{}-part-{}.json'.format(letter, dataset, str(part).zfill(4))
                    with open(file_name, 'w') as outfile:
                        json.dump(data, outfile)
                    _upload_file(file_name, bucket, os.path.join(s3_folder, dataset))

                    data = list()
                    part += 1

        file_name = '{}-{}-part-{}.json'.format(letter, dataset, str(part).zfill(4))
        with open(file_name, 'w') as outfile:
            json.dump(data, outfile)
        _upload_file(file_name, bucket, os.path.join(s3_folder, dataset))


def _get_s3_records(bucket_name, prefix):

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(name=bucket_name)

    return bucket.objects.filter(Prefix=prefix)


def _upload_file(file_name, bucket, prefix):
    s3 = boto3.resource("s3")
    s3_key = os.path.join(prefix, file_name)
    click.echo("Upload: " + file_name)
    s3.Bucket(bucket).upload_file(file_name, s3_key)
    os.remove(file_name)