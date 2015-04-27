{-# LANGUAGE OverloadedStrings  #-}

import Orientdb.Core

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
