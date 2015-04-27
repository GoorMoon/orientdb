{-# LANGUAGE OverloadedStrings #-}

{-
- Haskell driver for multi paradigm database OrientDB
-
- -}

module Orientdb.Core
(
  bufferLength,
  recvTimeOut,
  sendTimeOut,
  driverName,
  driverVersion,
  clientProtocolVersion,
  csvSerializer,
  OrientDBConnectionInfo(..),
  connectOrientDb,
  defaultOrientDbConnectionInfo,
  printConnection,
  closeConnection
) where

import Control.Monad (liftM)
import Control.Applicative ((<$>),(<*>))
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString (send,recvFrom)

import qualified Data.ByteString as BS (ByteString,length,unpack,empty,append)
import qualified Data.ByteString.UTF8 as U (toString)
import qualified Data.ByteString.Lazy as BSL (fromStrict,toStrict)
import Data.Word (Word8,Word16,Word32)
import Data.Int (Int8,Int16,Int32)
import Data.Binary.Put (Put,putWord8,putWord16be,putWord32be,putByteString,runPut)
import Data.Binary.Get (Get,getWord8,getWord16be,getWord32be,getByteString,runGet)
import Data.IORef

-- Global constants
bufferLength :: Int
bufferLength = 1024

recvTimeOut :: Int
recvTimeOut = 10000


sendTimeOut :: Int
sendTimeOut = 10000

driverName :: BS.ByteString
driverName = "haskell orientdb driver"

driverVersion :: BS.ByteString
driverVersion = "0.0.1"

clientProtocolVersion :: Word16
clientProtocolVersion = 21

csvSerializer :: BS.ByteString
csvSerializer = "ORecordDocument2csv"

-- Data Types
data OrientDBConnectionInfo = OrientDBConnectionInfo
  {
    orientDbHost          :: BS.ByteString,
    orientDbPort          :: Int,
    orientDbUser          :: BS.ByteString,
    orientDbPassword      :: BS.ByteString,
    orientDbDatabase      :: BS.ByteString,
    orientDbDatabaseType  :: BS.ByteString
  }

defaultOrientDbConnectionInfo :: OrientDBConnectionInfo
defaultOrientDbConnectionInfo = OrientDBConnectionInfo "127.0.0.1" 2424 "root" "" "" "document"

data Connection = Connection
  {
    sessionId :: IORef Int32,
    token :: BS.ByteString,
    configuration :: Response,
    connectionInfo :: OrientDBConnectionInfo,
    clientSocket :: Socket,
    protocolVersion :: Word16
  }

printConnection :: Connection -> IO ()
printConnection c = do
  sid <- printSessionId c
  putStrLn $ unwords ["<",U.toString . orientDbDatabase . connectionInfo $ c,
                    "(" ,U.toString . orientDbDatabaseType . connectionInfo $ c,
                    ")"," : ",sid,">"]
  where
    printSessionId :: Connection -> IO String
    printSessionId con = do
        sid <- readIORef (sessionId con)
        return $ show sid

data Cluster = Cluster
  {
    clusterName :: BS.ByteString,
    clusterId :: Int16,
    clusterType :: BS.ByteString,
    clusterDataSegmentId :: Int16
  } deriving (Show)

data Request = OpenDbRequest
data Response = Either (Int , String) Response |
  OpenDbResponse
  {
    clusters :: [Cluster],
    clustersConfig :: [Word8],
    orientDbRelease :: BS.ByteString
  } deriving (Show)

openDbRequestType,
  closeDbRequestType,
  commandRequestType
    :: Word8

openDbRequestType = 3
closeDbRequestType = 5
commandRequestType = 41

class RequestResponse a where
  execute :: a -> b


-- Operations
connectOrientDb :: OrientDBConnectionInfo -> IO Connection
connectOrientDb ci = do
  sock <- createSocket
  getServerAddr (U.toString $ orientDbHost ci) (orientDbPort ci) >>= 
    (\servAddr -> connect sock servAddr)
  version <-  recvFrom sock 2 >>= 
    (\(v , _ ) -> return $ runGet getWord16be (BSL.fromStrict v))
  let request = putOpenDbRequest ci
  --(print . BS.unpack) request
  _ <- send sock request
  response <- readAllContents sock
--  print $ BS.unpack response
  case getOpenDbResponse response of
      Left m -> error (U.toString m)
      Right (id , t , r) ->  do
        id' <- newIORef (fromIntegral id)
        return $! Connection
          {
            sessionId       = id',
            token           = t,
            configuration   = r,
            connectionInfo  = ci,
            clientSocket    = sock,
            protocolVersion = version
          }

putOpenDbRequest :: OrientDBConnectionInfo -> BS.ByteString
putOpenDbRequest ci = BSL.toStrict $ runPut (
  putWord8 openDbRequestType >>
  putWord32be (-1) >>
  putString driverName >>
  putString driverVersion >>
  putWord16be clientProtocolVersion >>
  putString BS.empty >>
  putString csvSerializer `putIf` (clientProtocolVersion > 21)  >>
  putWord8 0 `putIf` (clientProtocolVersion > 21) >>
  putString (orientDbDatabase ci) >>
  putString (orientDbDatabaseType ci) >>
  putString (orientDbUser ci) >>
  putString (orientDbPassword ci))


getOpenDbResponse :: BS.ByteString -> Either BS.ByteString (Int32 , BS.ByteString , Response)
getOpenDbResponse  = runGet (do
    status <- getWord8
    _ <- getWord32be
    if status /= 0
        then do
          errorMessage <- readError
          return $ Left errorMessage
        else do
          sessionid <- fromIntegral <$> getWord32be
          tokenSize <- (getWord32be `getIf`) (clientProtocolVersion > 26) 0
          token' <- (getByteString (fromIntegral tokenSize) `getIf`) (clientProtocolVersion > 26) ""
          clusters' <- readClusters
          clusterConfigLength <- fromIntegral <$> getWord32be :: Get Int32
          clusterConfig <- BS.unpack <$> (getByteString (fromIntegral clusterConfigLength) `getIf`) (clusterConfigLength > 0) ""
          orientDbReleaseLength <- getWord32be
          orientDbRelease' <- (getByteString (fromIntegral orientDbReleaseLength) `getIf`) (orientDbReleaseLength > 0) ""
          return $ Right (sessionid , token' , OpenDbResponse clusters' clusterConfig orientDbRelease')) . BSL.fromStrict 

closeConnection :: Connection -> IO ()
closeConnection conn = do
    sessionid <- readIORef (sessionId conn)
    let request = BSL.toStrict $ runPut (putWord8 closeDbRequestType >> putWord32be (fromIntegral sessionid))
    let sock = clientSocket conn
    _ <- send sock request
    shutdown sock ShutdownBoth
    close sock
    modifyIORef (sessionId conn) (\_ -> -1)
    return ()

runQuery :: Connection -> BS.ByteString -> IO ()
runQuery conn query = do
    sessionid <- readIORef (sessionId conn)
    let className = "q"
    let payload = BSL.toStrict $ runPut (createPayload query)
    let request = BSL.toStrict $ runPut (
                    putWord8 commandRequestType >>
                    putWord32be (fromIntegral sessionid) >>
                    putWord8 115 >>
                    putWord32be (payloadLength className query) >>
                    putString className >>
                    putByteString payload
                  )
    _ <- send (clientSocket conn) request
    response <- readAllContents (clientSocket conn)
    print $ BS.unpack response
    return ()
    -- Read Response
    where
      createPayload :: BS.ByteString -> Put
      createPayload q = (
        putString q >>
        putWord32be (-1) >>
        putString "" >>
        putWord32be 0)
      payloadLength :: BS.ByteString -> BS.ByteString -> Word32
      payloadLength cn q = fromIntegral (
        4 {- sizeof(int) -} +
        (BS.length cn) +
        4 {- sizeof(int) -} +
        (BS.length q) +
        4 {- sizeof(int) Non-Text Limit -} +
        4 {- sizeof(int) FetchPlan -} +
        4 {- sizeof(int) Serialized params length -})


readError :: Get BS.ByteString
readError = do
  followByte <- getWord8
  err <- readError' followByte ""
  if clientProtocolVersion >= 19
    then do
      serializedVersionLength <- fromIntegral <$> getWord32be :: Get Int32
      buffer <- (getByteString (fromIntegral serializedVersionLength) `getIf`) (serializedVersionLength > 0) ""
      return err
    else return err
  where
    readError' :: Word8 -> BS.ByteString -> Get BS.ByteString
    readError' 1 m = do
        exceptionClassLength <- getWord32be
        exceptionClassString <- getByteString (fromIntegral exceptionClassLength)
        exceptionMessageLength <- fromIntegral <$> getWord32be :: Get Int32
        if exceptionMessageLength /=  -1
            then do
                exceptionString <- getByteString (fromIntegral exceptionMessageLength)
                followByte <- fromIntegral <$> getWord8 :: Get Int8
                readError' (fromIntegral followByte) ((BS.append . BS.append exceptionClassString) ": " exceptionString)
            else do
                followByte <- fromIntegral <$> getWord8 :: Get Int8
                readError' (fromIntegral followByte) (BS.append exceptionClassString "\r\n")
    readError' _ m = return m

readClusters :: Get [Cluster]
readClusters = do
  clustersCount <- getWord16be
  readClusters' [] clustersCount
  where
    readClusters' :: [Cluster] -> Word16 -> Get [Cluster]
    readClusters' cs 0 = return cs
    readClusters' cs count = do
      cNameLength <- getWord32be
      cName <- getByteString (fromIntegral cNameLength)
      cId <- getWord16be
      cTypeLength <- (getWord32be `getIf`) (clientProtocolVersion < 24) 0
      cType <- (getByteString (fromIntegral cTypeLength) `getIf`) (clientProtocolVersion < 24) ""
      cDataSegment <- (getWord16be `getIf`) (clientProtocolVersion >= 12) 0
      readClusters' (Cluster cName (fromIntegral cId) cType (fromIntegral cDataSegment) : cs) (count - 1)


readAllContents :: Socket -> IO BS.ByteString
readAllContents sock = readAllContents' ""
  where
      readAllContents' m = do
          (b , _) <- recvFrom sock bufferLength
          if BS.length b < bufferLength
              then return $ BS.append m b
              else readAllContents' (BS.append m b)

putIf :: Put -> Bool -> Put
putIf p c
  | c = p
  | otherwise = putByteString BS.empty

getIf :: Get a -> Bool -> a -> Get a
getIf g c d
  | c = g
  | otherwise =  return d

putString :: BS.ByteString -> Put
putString str = putWord32be (fromIntegral $ BS.length str) >> putByteString str

createSocket :: IO Socket
createSocket = socket AF_INET Stream defaultProtocol >>=
        (\ sock ->
          setSocketOption sock RecvTimeOut recvTimeOut >>
          setSocketOption sock SendTimeOut sendTimeOut >>
          setSocketOption sock KeepAlive   1 >>
          return sock )

getServerAddr :: String -> Int -> IO SockAddr
getServerAddr h p = (addrAddress . head) <$> (getAddrInfo Nothing (Just h) (Just . show $ p))
