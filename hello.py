

import time,os,sys,argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='emr submit')
    parser.add_argument('--msg',help='submit msg', required=False)

    args = parser.parse_args()
    msg = args.msg
    if not msg:
        msg = "world"
    print("hello %s!" % (msg,))