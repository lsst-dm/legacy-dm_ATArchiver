#!/usr/bin/env python3
import argparse
import os
import sys
from lsst.ts import salobj


class Commander:

    def emit_startIntegration(self, args):
        cam = salobj.Controller(name="ATCamera", index=0)
        kwInt = {'imageSequenceName': str(args.imageSequenceName),
                 'imagesInSequence': args.imagesInSequence,
                 'imageName': str(args.imageName),
                 # 'imageIndex': args.imageIndex,
                 'imageSource': str(args.imageSource),
                 'imageController': str(args.imageController),
                 'imageDate': str(args.imageDate),
                 'imageNumber': args.imageNumber,
                 'timeStampAcquisitionStart': args.timeStampAcquisitionStart,
                 'exposureTime': args.exposureTime,
                 'priority': 1}
        cam.evt_startIntegration.set_put(**kwInt)

    def emit_endReadout(self, args):
        cam = salobj.Controller(name="ATCamera", index=0)
        kwInt = {'imageSequenceName': str(args.imageSequenceName),
                 'imagesInSequence': args.imagesInSequence,
                 'imageName': str(args.imageName),
                 # 'imageIndex': args.imageIndex,
                 'imageSource': str(args.imageSource),
                 'imageController': str(args.imageController),
                 'imageDate': str(args.imageDate),
                 'imageNumber': args.imageNumber,
                 'timeStampAcquisitionStart': args.timeStampAcquisitionStart,
                 'exposureTime': args.exposureTime,
                 'priority': 1}
        cam.evt_endReadout.set_put(**kwInt)

    def emit_largeFileObjectAvailable(self, args):
        cam = salobj.Controller(name="EFD", index=0)
        kwInt = {'byteSize': args.byteSize,
                 'checkSum': args.checkSum,
                 'generator': args.generator,
                 'mimeType': args.mimeType,
                 'url': args.url,
                 'version': args.version,
                 'id': args.identifier}
        cam.evt_largeFileObjectAvailable.set_put(**kwInt)


if __name__ == "__main__":

    name = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=name, description="Send SAL events applicable to ATArchiver")

    subparsers = parser.add_subparsers(dest="event")

    start_parser = subparsers.add_parser('startIntegration')
    start_parser.add_argument('-q', '--imageSequenceName', dest='imageSequenceName', required=True)
    start_parser.add_argument('-s', '--imagesInSequence', dest='imagesInSequence', type=int, required=True)
    start_parser.add_argument('-n', '--imageName', dest='imageName', required=True)
    start_parser.add_argument('-i', '--imageIndex', dest='imageIndex', required=True)

    start_parser.add_argument('-S', '--imageSource', dest='imageSource', required=True)
    start_parser.add_argument('-c', '--imageController', dest='imageController', required=True)

    start_parser.add_argument('-t', '--imageDate', dest='imageDate', required=True)
    start_parser.add_argument('-m', '--imageNumber', dest='imageNumber', type=int, required=True)
    start_parser.add_argument('-T', '--timeStampAcquisitionStart', dest='timeStampAcquisitionStart',
                              type=float, required=True)
    start_parser.add_argument('-e', '--exposureTime', dest='exposureTime', type=float, required=True)

    end_parser = subparsers.add_parser('endReadout')
    end_parser.add_argument('-q', '--imageSequenceName', dest='imageSequenceName', required=True)
    end_parser.add_argument('-s', '--imagesInSequence', dest='imagesInSequence', type=int, required=True)
    end_parser.add_argument('-n', '--imageName', dest='imageName', required=True)
    end_parser.add_argument('-i', '--imageIndex', dest='imageIndex', required=True)

    end_parser.add_argument('-S', '--imageSource', dest='imageSource', required=True)
    end_parser.add_argument('-c', '--imageController', dest='imageController', required=True)

    end_parser.add_argument('-t', '--imageDate', dest='imageDate', required=True)
    end_parser.add_argument('-m', '--imageNumber', dest='imageNumber', type=int, required=True)
    end_parser.add_argument('-T', '--timeStampAcquisitionStart', dest='timeStampAcquisitionStart',
                            type=float, required=True)
    end_parser.add_argument('-e', '--exposureTime', dest='exposureTime', type=float, required=True)

    large_parser = subparsers.add_parser('largeFileObjectAvailable')
    large_parser.add_argument('-b', '--byteSize', dest='byteSize', type=int, required=True)
    large_parser.add_argument('-c', '--checkSum', dest='checkSum', type=str, required=True)
    large_parser.add_argument('-g', '--generator', dest='generator', type=str, default='ATHeaderService')
    large_parser.add_argument('-m', '--mimeType', dest='mimeType', type=str, default='raw')
    large_parser.add_argument('-u', '--url', dest='url', type=str, required=True)
    large_parser.add_argument('-v', '--version', dest='version', type=float, required=True)
    large_parser.add_argument('-i', '--id', dest='identifier', type=str, required=True)

    args = parser.parse_args()

    cmdr = Commander()
    execute = getattr(cmdr, f"emit_{args.event}")
    execute(args)
