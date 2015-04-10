{-# LANGUAGE OverloadedStrings  #-}

import Orientdb.Core

-- Data Types

main :: IO ()
main = do
  conn <- connectOrientDb
    defaultOrientDbConnectionInfo {
      orientDbUser = "root",
      orientDbPassword ="root",
      orientDbDatabase = "MovieRatings"}
  printConnection conn
  closeConnection conn
  return ()
