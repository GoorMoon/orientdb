{-# LANGUAGE OverloadedStrings  #-}

import Orientdb.Core 

-- Data Types

main :: IO ()
main = do
  conn <- connectOrientDb defaultOrientDbConnectionInfo { orientDbUser = "root", orientDbPassword ="root",orientDbDatabase = "Temp1"}
  print conn

