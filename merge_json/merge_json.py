from retrying import retry
import os
import json
import click
import boto3
import botocore
import urllib3


@click.command()
@click.argument("bucket")
@click.argument("folder")
def cli(bucket, folder):
    datasets = ["iso", "adm1", "adm2"]

    for dataset in datasets:
        part = 0
        data = list()

        prefix = os.path.join(folder, "api", dataset)
        click.echo("Searching: " + prefix)

        for obj in _get_s3_records(bucket, prefix):

            filename, file_extension = os.path.splitext(obj.key)
            if file_extension == ".txt":
                click.echo("Download: " + os.path.basename(obj.key))
                data += json.loads(obj.get()["Body"].read().decode("utf-8"))
                l = len(data)
                if l > 200000:
                    file_name = "{}-part-{}.json".format(
                        dataset, str(part).zfill(4)
                    )
                    click.echo("Write file {} with {} rows".format(file_name, l))
                    with open(file_name, "w") as outfile:
                        json.dump(data, outfile)
                    _upload_file(
                        file_name, bucket, os.path.join(folder, dataset)
                    )

                    data = list()
                    part += 1

        file_name = "{}-part-{}.json".format(dataset, str(part).zfill(4))
        with open(file_name, "w") as outfile:
            json.dump(data, outfile)
        _upload_file(file_name, bucket, os.path.join(folder, dataset))


def retry_if_timeout(exception):
    """Return True if we should retry (in this case when it's an IOError), False otherwise"""
    return isinstance(exception, (botocore.exceptions.ReadTimeoutError, urllib3.exceptions.ReadTimeoutError))


@retry(retry_on_exception=retry_if_timeout, wait_fixed=2000)
def _get_s3_records(bucket_name, prefix):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(name=bucket_name)

    return bucket.objects.filter(Prefix=prefix)


@retry(retry_on_exception=retry_if_timeout, wait_fixed=2000)
def _upload_file(file_name, bucket, prefix):
    s3 = boto3.resource("s3")
    s3_key = os.path.join(prefix, file_name)
    click.echo("Upload: " + file_name)
    s3.Bucket(bucket).upload_file(file_name, s3_key)
    os.remove(file_name)
