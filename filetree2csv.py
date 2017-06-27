#!/usr/bin/python
# coding=utf8


import os
import csv
import argparse

argparser = argparse.ArgumentParser(description='CREATE CSV')
argparser.add_argument('-i', '--input', default='.',           help='Set Top Level Directory')
argparser.add_argument('-o', '--output', default='struct.csv',    help='Name of output file')
args=argparser.parse_args()

with open(args.output, 'wb') as output:
    fieldnames=['schlagwoerter','filename_and_path']
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writerow(
    {
        'schlagwoerter': 'schlagwoerter',
        'filename_and_path': 'filename_and_path'
    })
    startpath=os.path.abspath(args.input)

    for root, dirs, files in os.walk(startpath):
        for file in files:
            if file==os.path.basename(__file__) or file==args.output:
                continue
            writer.writerow(
            {
                'schlagwoerter': root.replace(startpath, "")[1:],
                'filename_and_path': os.path.join(root.replace(startpath, "")[1:],file)
            })
