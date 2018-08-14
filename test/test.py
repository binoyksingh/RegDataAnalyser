# -*- coding: UTF-8 -*-
import string

class Person:
  def __init__(self):
    self.name = "myconnectionsstring"

  def myfunc(self):
    str = "BON TESORO P¿LICO   0,400 04/2022"
    str = str.decode('ascii',errors='ignore')
    print (str)

p1 = Person()
p1.myfunc()