import boto3
from typing import List
import copy

def aws_s3_ls_prefix(s3, bucket, prefix):
    marker = ''
    l_prefix = []
    res = s3.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker, Delimiter='/')
    if 'CommonPrefixes' not in res:
        return l_prefix
    l_prefix.extend( [content['Prefix'] for content in res['CommonPrefixes']] )
    return l_prefix

def aws_s3_ls_prefix_r(s3, bucket, prefix):
    marker = ''
    l_ret = []
    
    l_target = aws_s3_ls_prefix(s3, bucket, prefix)
    l_ret.extend(copy.deepcopy(l_target))
    while True:
        if len(l_target)==0:
            break
        tgt = l_target[-1]
        l_target.pop()
        l_tmp = aws_s3_ls_prefix(s3, bucket, prefix=tgt)
        l_target.extend(copy.deepcopy(l_tmp))
        l_ret.extend(copy.deepcopy(l_tmp))
    l_ret.sort()
    return l_ret

def main():
    bucket='test-9999'
    prefix=''
    s3 = boto3.client('s3')
    l_prefix = aws_s3_ls_prefix_r(s3, bucket, prefix)
    print(l_prefix)


main()

