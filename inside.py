import sys

def main():
  if (hasattr(sys, 'real_prefix') or hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
    print('inside virtual env')
  else:
    print('outside virtual env')

if __name__ == "__main__":
  main()