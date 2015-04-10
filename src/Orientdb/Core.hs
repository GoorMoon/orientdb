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
  defaultOrientDbConnectionInfo
) where

import Control.Monad (liftM)
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket (Socket(..),SockAddr(..),getAddrInfo,AddrInfo(..),SocketOption(..),setSocketOption,
                       defaultProtocol,Family(..),SocketType(..),socket,connect)

import Network.Socket.ByteString (send,recvFrom)

import qualified Data.ByteString as BS (ByteString(..),length,unpack,empty,append)
import qualified Data.ByteString.UTF8 as U (toString)
import qualified Data.ByteString.Lazy as BSL (fromStrict,toStrict)
import Data.Word (Word8(..),Word16(..),Word32(..))
import Data.Int (Int8(..),Int16(..),Int32(..))
import Data.Binary.Put (Put(..),putWord8,putWord16be,putWord32be,putByteString,runPut)
import Data.Binary.Get (Get(..),getWord8,getWord16be,getWord32be,getByteString,runGet)
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

--class ShowIO a where
--    showIO :: a -> IO String

--instance ShowIO a where
--    showIO = return . show

--instance Show Connection where
--  show c = unwords ["<",U.toString . orientDbDatabase . connectionInfo $ c,
--                    "(" ,U.toString . orientDbDatabaseType . connectionInfo $ c,
--                    ")"," : ",show . sessionId $ c,">"]

printSessionId :: Connection -> IO ()
printSessionId con = do
    sid <- readIORef (sessionId con)
    print sid

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
  -- Try to rewrite with Control.Applicative
  sock <- createSocket
  servAddr <- getServerAddr (U.toString $ orientDbHost ci) (orientDbPort ci)
  connect sock servAddr
  version <-  recvFrom sock 2 >>= (\(v , _ ) ->
    return $ runGet getWord16be (BSL.fromStrict v))
  let request = putOpenDbRequest ci
  --print $ dumpByteString (BSL.toStrict request)
  _ <- send sock request
  response <- readAllContents sock
--  print $ BS.unpack response
  case getOpenDbResponse response of
      Left m -> error (U.toString m)
      Right (id' , t , r) ->  do
        id'' <- newIORef (fromIntegral id')
        return $! Connection
          {
            sessionId       = id'',
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
getOpenDbResponse response = runGet (do
    status <- getWord8
    _ <- getWord32be
    if status /= 0
        then do 
          errorMessage <- readError
          return $ Left errorMessage
        else do
          sessionid <- liftM fromIntegral getWord32be
          tokenSize <- if clientProtocolVersion > 26 then getWord32be else return 0
          token' <- if clientProtocolVersion > 26 then getByteString (fromIntegral tokenSize) else return ""
          clusters' <- readClusters
          clusterConfigLength <- liftM fromIntegral getWord32be :: Get Int32
          clusterConfig <- liftM BS.unpack $ if clusterConfigLength > 0 then getByteString (fromIntegral clusterConfigLength) else return ""
          orientDbReleaseLength <- getWord32be
          orientDbRelease' <- if orientDbReleaseLength > 0 then getByteString (fromIntegral orientDbReleaseLength) else return ""
          return $ Right (sessionid , token' , OpenDbResponse clusters' clusterConfig orientDbRelease')) (BSL.fromStrict response)

closeConnection :: Connection -> IO ()
closeConnection conn = do
    sessionid <- readIORef (sessionId conn)
    let  request = liftM BSL.toStrict runPut (putWord8 closeDbRequestType >> putWord32be (fromIntegral sessionid))
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
      payloadLength cn q = fromIntegral (
        4 {- sizeof(int) -} + 
        (BS.length cn) + 
        4 {- sizeof(int) -} + 
        (BS.length q) + 
        4 {- sizeof(int) Non-Text Limit -} + 
        4 {- sizeof(int) FetchPlan -} + 
        4 {- sizeof(int) Serialized params length -})
--      createPayload :: BS.ByteString -> BS.ByteString
      createPayload q = (
        putString q >>
        putWord32be (-1) >>
        putString "" >>
        putWord32be 0)


readError :: Get BS.ByteString
readError = do
  followByte <- getWord8 
  err <- readError' followByte ""
  if clientProtocolVersion >= 19
    then do
      serializedVersionLength <- liftM fromIntegral getWord32be :: Get Int32
      buffer <- if (serializedVersionLength > 0) then getByteString (fromIntegral serializedVersionLength) else return ""
      return err
    else return err
  where
    readError' :: Word8 -> BS.ByteString -> Get BS.ByteString
    readError' 1 m = do
        exceptionClassLength <- getWord32be
        exceptionClassString <- getByteString (fromIntegral exceptionClassLength)
        exceptionMessageLength <- liftM fromIntegral getWord32be :: Get Int32
        if exceptionMessageLength /=  -1
            then do
                exceptionString <- getByteString (fromIntegral exceptionMessageLength)
                followByte <- liftM fromIntegral getWord8 :: Get Int8
                readError' (fromIntegral followByte) ((BS.append . BS.append exceptionClassString) ": " exceptionString)
            else do
                followByte <- liftM fromIntegral getWord8 :: Get Int8
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
      cTypeLength <- if clientProtocolVersion < 24 then getWord32be else return 0
      cType <- if clientProtocolVersion < 24 then getByteString (fromIntegral cTypeLength) else return ""
      cDataSegment <- if clientProtocolVersion >= 12 then getWord16be else return 0
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

getIf :: (Num a) => Get a -> Bool -> Get a
getIf p c
  | c = p
  | otherwise =  return 0

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
getServerAddr h p = liftM (addrAddress . head) (getAddrInfo Nothing (Just h) (Just . show $ p))
