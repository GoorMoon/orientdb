{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE DataKinds #-}

{-
- Haskell driver for multi paradigm database OrientDB
-
- -}
import Control.Monad (liftM)
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString hiding (recv)
import Network.Socket.ByteString.Lazy
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.UTF8 as U
import Prelude hiding (getContents)
import Data.Binary.Get
import Data.Binary.Put
import Data.Word
import Data.Int

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
    sessionId :: Int32,
    token :: BS.ByteString,
    configuration :: Response,
    connectionInfo :: OrientDBConnectionInfo,
    clientSocket :: Socket,
    protocolVersion :: Word16,
    run :: String -> IO (),
    disconect :: IO ()
  }

instance Show Connection where
  show c = "<"
    ++ (U.toString . orientDbDatabase . connectionInfo $ c)
    ++ "(" ++ (U.toString . orientDbDatabaseType . connectionInfo $ c) ++ ")"
    ++ " : " ++ (show . sessionId $ c) ++ ">"

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

--type RequestType = Int

openDbRequestType :: Word8
openDbRequestType = 3


class RequestResponse a where
  execute :: a -> b

connectOrientDb :: OrientDBConnectionInfo -> IO Connection
connectOrientDb ci = do
  -- Try to rewrite with Comtrol.Applicative
  sock <- createSocket
  sockAddr <- getServerAddr (U.toString $ orientDbHost ci) (orientDbPort ci)
  connect sock sockAddr
  version <-  recvFrom sock 2 >>= (\(v , _ ) ->
    return $ runGet getWord16be (BSL.fromStrict v))
  let request = putOpenDbRequest ci
  --print $ dumpByteString (BSL.toStrict request)
  _ <- send sock request
  response <- readAllContents sock
  print $ BS.unpack response
  case getOpenDbResponse response of
      Left m -> error (U.toString m)
      Right (id' , t , r) ->  return $! Connection
        {
          sessionId       = fromIntegral id',
          token           = t,
          configuration   = r,
          connectionInfo  = ci,
          clientSocket    = sock,
          protocolVersion = version,
          run             = \_ -> return (),
          disconect       = return ()
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


getOpenDbResponse :: BS.ByteString -> Either BS.ByteString (Word32 , BS.ByteString , Response)
getOpenDbResponse response = runGet (do
    status <- getWord8
    _ <- getWord32be
    if status /= 0
        then do
          errorMessage <- readError
          return $ Left errorMessage
        else do
          sessionid <- getWord32be
          tokenSize <- if clientProtocolVersion > 26 then getWord32be else return 0
          token' <- if clientProtocolVersion > 26 then getByteString (fromIntegral tokenSize) else return ""
          clusters' <- readClusters
          clusterConfigLength <- liftM fromIntegral getWord32be :: Get Int32
          clusterConfig <- liftM BS.unpack $ if clusterConfigLength > 0 then getByteString (fromIntegral clusterConfigLength) else return ""
          orientDbReleaseLength <- getWord32be
          orientDbRelease' <- if orientDbReleaseLength > 0 then getByteString (fromIntegral orientDbReleaseLength) else return ""
          return $ Right (sessionid , token' , OpenDbResponse clusters' clusterConfig orientDbRelease')) (BSL.fromStrict response)

readError :: Get BS.ByteString
readError = do
  followByte <- liftM fromIntegral getWord8 :: Get Int8
  err <- readError' followByte ""
  if clientProtocolVersion >= 19
    then do
      serializedVersionLength <- liftM fromIntegral getWord32be :: Get Int32
      --buffer <- if (serializedVersionLength > 0) then getByteString (fromIntegral serializedVersionLength) else return ""
      return err
    else return err
  where
    readError' :: Int8 -> BS.ByteString -> Get BS.ByteString
    readError' 1 m = do
        exceptionClassLength <- getWord32be
        exceptionClassString <- getByteString (fromIntegral exceptionClassLength)
        exceptionMessageLength <- liftM fromIntegral getWord32be :: Get Int32
        if exceptionMessageLength /=  -1
            then do
                exceptionString <- getByteString (fromIntegral exceptionMessageLength)
                followByte <- liftM fromIntegral getWord8 :: Get Int8
                readError' (fromIntegral followByte) (BS.append exceptionClassString exceptionString)
            else do
                followByte <- liftM fromIntegral getWord8 :: Get Int8
                readError' (fromIntegral followByte) exceptionClassString
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

dumpByteString :: BS.ByteString -> [Word8]
dumpByteString = BS.unpack

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

main :: IO ()
main = undefined
