#!/usr/bin/env python

#-----------------------------------------------------------------------------------------------------
# Created By  : Marc Henderson
# Created Date: 2022/08/17
# version ='1.1'
# ----------------------------------------------------------------------------------------------------
""" Simple macro-replacing tool that allows script runners to 
      replace macros with values in parameter file and run scripts elsewhere
      Instead of using SnowSQL

  Args:
      config_file:   config file used for parameter substitution (relative or absolute path)
      input_path:    directory containing files for substitution (relative or absolute path)
      output_path:   directory containing files with macros replaced (relative or absolute path)

  Usage:
      python <path_to_script>/param_sub.py <path_to_config_file>/config.txt <input_path> <output_path>
  
  Returns:
      None
  """
# ----------------------------------------------------------------------------------------------------
# SUMMARY OF CHANGES
# Date(yyyy-mm-dd)    Author                              Comments
# ------------------- -------------------                 --------------------------------------------
# 2022-08-17          Marc Henderson                      Initial build
# 2022-09-20          Marc Henderson                      Made input/output paths relative to working
#                                                         directory.
# 2024-01-04          Marc Henderson                      Added replace statement for macros located
#                                                         in strings (i.e. 'SOME_&{MACRO}_NAME')
# ----------------------------------------------------------------------------------------------------

import os
import sys

def main():

  my_path = os.path.abspath(os.path.dirname(__file__))

  # assign params
  config_file = sys.argv[1]
  input_path = sys.argv[2]
  output_path = sys.argv[3]

  #empty dictionary
  d = {}

  #read config file and create dictionary of macros and values
  config_file_path = os.path.join(my_path, config_file)
  cf = open(config_file_path, 'r')

  print('\nThe following macros will be placed with the following values:')
  for ln in cf:
      kv = ln.strip().split('=')
      k = kv[0].strip()
      v = kv[1].strip()
      d[k] = v
      
      print(k, ' -> ', v)

  print('\n')
  cf.close()

  #create input path relative to working directory
  input_path_r = os.path.join(my_path, input_path)

  #create output path relative to working directory
  output_path_r = os.path.join(my_path, output_path)

  #for each file in input_path replace macro key (k) with value (v) and write new file to output_path
  for fn in os.listdir(input_path_r):
      f = os.path.join(input_path_r, fn)
      
      otup = os.path.splitext(fn)
      ofn = otup[0] + "_sub" + otup[1]
      of = os.path.join(output_path_r, ofn)

      # check if it is a file
      if os.path.isfile(f):

          # Read in the file
          with open(f, 'r') as fr :
            fd = fr.read()

            #Replace &&&& (escaped JavaScript logical AND)
            fd = fd.replace("&&&&", "&&")

            for k in d:
              # Replace k with v
              rk = "&"+k
              rk_alt = "&{"+k+"}" #when macro is embedded in string
              fd = fd.replace(rk, d[k])
              fd = fd.replace(rk_alt, d[k])
            fr.close()

          # Write the file out again
          with open(of, 'w') as fw:
            fw.write(fd)
            fw.close()

  print(f'Files generated.  Check directory: {output_path}')

if __name__ == "__main__":
    main()
