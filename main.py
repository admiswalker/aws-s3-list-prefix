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

def remain_bottom(l_prefix):
    l = []
    for i in range(len(l_prefix)-1):
        if l_prefix[i] in l_prefix[i+1]:
            continue
        l.append(l_prefix[i])
    if l_prefix[-2] not in l_prefix[-1]:
        l.append(l_prefix[-1])
    return l

def print_l(l):
    for i in range(len(l)):
        print(l[i])
    return

def get_first_object_name(s3, bucket, prefix):
    marker = ''
    ret = s3.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker, MaxKeys=1)
    l = [content['Key'] for content in ret['Contents']]
    return l

def main():
    bucket='test-9999'
    prefix=''
    s3 = boto3.client('s3')
    l_prefix = aws_s3_ls_prefix_r(s3, bucket, prefix)
    
    print_l(l_prefix)
    print()
    
    l = remain_bottom(l_prefix)
    print_l(l)
    
    print()
    print()
    #print(l[0])
    #name = get_first_object_name(s3, bucket, l[0])
    #print(name)
    for i in range(len(l)):
        name = get_first_object_name(s3, bucket, l[i])
        print(name)


main()

