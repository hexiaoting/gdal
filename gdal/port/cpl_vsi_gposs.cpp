#include "oss.h"

#if HAVE_FCNTL_H
#  include <fcntl.h>
#endif
#if HAVE_SYS_STAT_H
#  include <sys/stat.h>
#endif
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <ctime>
#include <algorithm>
#include <map>
#include <string>
#include <utility>

#include "cpl_gposs.h"
#include "cpl_port.h"
#include "cpl_vsi.h"
#include "cpl_vsi_virtual.h"
#include "cpl_atomic_ops.h"
#include "cpl_conv.h"
#include "cpl_error.h"
#include "cpl_multiproc.h"
#include "cpl_string.h"

CPL_CVSID("$Id$")

class VSIGPOssFile
{
    CPL_DISALLOW_COPY_ASSIGN(VSIGPOssFile)

public:
    CPLString     osFilename{};
    int  nRefCount = 0;
    bool          bIsDirectory = false;
    GByte        *pabyData = nullptr;
    vsi_l_offset  nLength = 0;
    vsi_l_offset  nMaxLength = GUINTBIG_MAX;
    time_t        mTime = 0;

    VSIGPOssFile();
    virtual ~VSIGPOssFile();
};

/************************************************************************/
/* ==================================================================== */
/*                             VSIGPOssHandle                             */
/* ==================================================================== */
/************************************************************************/

class VSIGPOssFilesystemHandler;

class VSIGPOssHandle final : public VSIVirtualHandle
{

  protected:
    VSIGPOssFilesystemHandler    *poFS;
    bool            bIsDirectory = false;
    char*           m_pszURL;

  public:
    ossContext context;
    VSIGPOssHandleHelper* m_poOSSHandleHelper;
    VSIGPOssFile    *poFile = nullptr;
    vsi_l_offset  m_nOffset = 0;
    bool          bUpdate = false;
    bool          bEOF = false;

    VSIGPOssHandle() = default;
    VSIGPOssHandle( VSIGPOssFilesystemHandler* poFSIn,
	const char* pszURLIn, VSIGPOssHandleHelper* poGpOssHandleHelper); 
    ~VSIGPOssHandle() override = default;

    int Seek( vsi_l_offset nOffset, int nWhence ) override;
    vsi_l_offset Tell() override;
    size_t Read( void *pBuffer, size_t nSize,
                    size_t nMemb ) override;
    size_t Write( const void *pBuffer, size_t nSize,
                     size_t nMemb ) override;
    int Eof() override;
    int Close() override;
    int Truncate( vsi_l_offset nNewSize ) override;
};

/************************************************************************/
/* ==================================================================== */
/*                       VSIGPOssFilesystemHandler                        */
/* ==================================================================== */
/************************************************************************/

class VSIGPOssFilesystemHandler final : public VSIFilesystemHandler
{
    CPL_DISALLOW_COPY_ASSIGN(VSIGPOssFilesystemHandler)
    //CPL_DISALLOW_COPY_ASSIGN(ossContext)

  protected:
	CPLString GetFSPrefix() { return "/vsigposs/"; }
	VSIGPOssHandle* CreateFileHandle(
	                const char* pszUnprefixed );

  public:
    std::map<CPLString, VSIGPOssFile*> oFileList{};
    CPLMutex        *hMutex = nullptr;

    VSIGPOssFilesystemHandler() = default;
    ~VSIGPOssFilesystemHandler() override;

    // TODO(schwehr): Fix VSIFileFromMemBuffer so that using is not needed.
    using VSIFilesystemHandler::Open;

    VSIVirtualHandle *Open( const char *pszFilename,
                            const char *pszAccess,
                            bool bSetError ) override;
    int Stat( const char *pszFilename, VSIStatBufL *pStatBuf,
              int nFlags ) override;
    int Unlink( const char *pszFilename ) override;
    int Mkdir( const char *pszDirname, long nMode ) override;
    int Rmdir( const char *pszDirname ) override;
    char **ReadDirEx( const char *pszDirname,
                      int nMaxFiles ) override;
    int Rename( const char *oldpath,
                const char *newpath ) override;
    GIntBig  GetDiskFreeSpace( const char* pszDirname ) override;

    static std::string NormalizePath( const std::string &in );

    int              Unlink_unlocked( const char *pszFilename );
};


/************************************************************************/
/*                             VSIGPOssFile()                             */
/************************************************************************/

VSIGPOssFile::VSIGPOssFile()
{
    time(&mTime);
}

/************************************************************************/
/*                            ~VSIGPOssFile()                             */
/************************************************************************/

VSIGPOssFile::~VSIGPOssFile()

{
    if( nRefCount != 0 )
        CPLError( CE_Warning, CPLE_AppDefined,
                  "Memory file %s deleted with %d references.",
                  osFilename.c_str(), nRefCount );

    if( pabyData )
        CPLFree( pabyData );
}

/************************************************************************/
/* ==================================================================== */
/*                             VSIGPOssHandle                             */
/* ==================================================================== */
/************************************************************************/

VSIGPOssHandle::VSIGPOssHandle( VSIGPOssFilesystemHandler* poFSIn,
	const char* pszURLIn, VSIGPOssHandleHelper* poGpOssHandleHelper) :
    poFS(poFSIn),
    bIsDirectory(false),
    m_poOSSHandleHelper(poGpOssHandleHelper)
{
    m_pszURL = CPLStrdup(pszURLIn);
    context = ossInitContext("S3",
	    m_poOSSHandleHelper->GetRegion(),
	    NULL,
	    m_poOSSHandleHelper->GetAccessKey(),
	    m_poOSSHandleHelper->GetSecretAccessKey(),
	    static_cast<int64_t>(1024), static_cast<int64_t>(1024));
    if (context == NULL)
	CPLError(CE_Failure, CPLE_AppDefined,
		"Init Context for /vsigposs failed.");
}

/************************************************************************/
/*                               Close()                                */
/************************************************************************/

int VSIGPOssHandle::Close()

{
    if( CPLAtomicDec(&(poFile->nRefCount)) == 0 )
        delete poFile;

    poFile = nullptr;
    return 0;
}

/************************************************************************/
/*                                Seek()                                */
/************************************************************************/

int VSIGPOssHandle::Seek( vsi_l_offset nOffset, int nWhence )

{
    if( nWhence == SEEK_CUR )
    {
        if( nOffset > INT_MAX )
        {
            //printf("likely negative offset intended\n");
        }
        m_nOffset += nOffset;
    }
    else if( nWhence == SEEK_SET )
    {
	if (nOffset == m_nOffset)
	    return 0;
        m_nOffset = nOffset;
    }
    else if( nWhence == SEEK_END )
    {
        m_nOffset = poFile->nLength + nOffset;
    }
    else
    {
        errno = EINVAL;
        return -1;
    }

    bEOF = false;

    if( m_nOffset > poFile->nLength )
    {
        if( bUpdate ) // Writable files are zero-extended by seek past end.
        {
	    CPLError(CE_Failure, CPLE_AppDefined,
		    "##Seek## Only read-only mode is supported for /vsigposs");
	}
    }

    return 0;
}

/************************************************************************/
/*                                Tell()                                */
/************************************************************************/

vsi_l_offset VSIGPOssHandle::Tell()

{
    return m_nOffset;
}

/************************************************************************/
/*                                Read()                                */
/************************************************************************/

size_t VSIGPOssHandle::Read( void * pBuffer, size_t nSize, size_t nCount )

{
    size_t nBytesToRead = nSize * nCount;
    if( nCount > 0 && nBytesToRead / nCount != nSize )
    {
        bEOF = true;
        return 0;
    }

    //TODO : if is a directory  return 0
    if( poFile == nullptr) 
    {
        bEOF = true;
        return 0;
    }
    if (poFile->bIsDirectory)
	return 0;
    if( poFile->nLength <= m_nOffset ||
        nBytesToRead + m_nOffset < nBytesToRead )
    {
        bEOF = true;
        return 0;
    }
    if( nBytesToRead + m_nOffset > poFile->nLength )
    {
        nBytesToRead = static_cast<size_t>(poFile->nLength - m_nOffset);
        nCount = nBytesToRead / nSize;
        bEOF = true;
    }

    if( nBytesToRead )
    {
	if (poFile->pabyData == nullptr) {
	    //Read from oss
	    int64_t idx = 0, size = poFile->nLength;
	    poFile->pabyData = static_cast<GByte*>(VSIMalloc( size + 1));
	    if (poFile->pabyData == nullptr)
		    CPLError( CE_Failure, CPLE_AppDefined,
			    "VSIMalloc pabyData failed "
			    );
	    ossObject ossObjectInt = ossGetObject(context,
		    m_poOSSHandleHelper->GetBucket(),
		    m_poOSSHandleHelper->GetObjectKey(),
		    0,
		    size);
	    if (ossObjectInt == NULL) {
		printf("ossGetObject %s failed.\n", m_poOSSHandleHelper->GetObjectKey().c_str());
	    }

	    while (idx < size){
		int32_t tmp;
		tmp = ossRead(context, ossObjectInt, poFile->pabyData + idx, size - idx);
		if (tmp == -1) {
		    CPLError( CE_Failure, CPLE_AppDefined,
			    "ossRead Object failed %s",
			    ossGetLastError());
		} else if (tmp > 0) {
		    idx += tmp;
		}
	    }
	}
        memcpy( pBuffer, poFile->pabyData + m_nOffset,
                static_cast<size_t>(nBytesToRead) );
    }
    m_nOffset += nBytesToRead;

    return nCount;
}

/************************************************************************/
/*                               Write()                                */
/************************************************************************/

size_t VSIGPOssHandle::Write( const void * pBuffer, size_t nSize, size_t nCount )

{
    CPLError(CE_Failure, CPLE_AppDefined,
	    "##Write## Only read-only mode is supported for /vsigposs");
    if( !bUpdate )
    {
        errno = EACCES;
        return 0;
    }

    const size_t nBytesToWrite = nSize * nCount;
    if( nCount > 0 && nBytesToWrite / nCount != nSize )
    {
        return 0;
    }
    if( nBytesToWrite + m_nOffset < nBytesToWrite )
    {
        return 0;
    }

    if( nBytesToWrite )
        memcpy( poFile->pabyData + m_nOffset, pBuffer, nBytesToWrite );
    m_nOffset += nBytesToWrite;

    time(&poFile->mTime);

    return nCount;
}

/************************************************************************/
/*                                Eof()                                 */
/************************************************************************/

int VSIGPOssHandle::Eof()

{
    return bEOF;
}

/************************************************************************/
/*                             Truncate()                               */
/************************************************************************/

int VSIGPOssHandle::Truncate( vsi_l_offset nNewSize )
{
    CPLError(CE_Failure, CPLE_AppDefined,
	    "##Truncate %lld## Only read-only mode is supported for /vsigposs", nNewSize);
    if( !bUpdate )
    {
        errno = EACCES;
        return -1;
    }

    return -1;
}

/************************************************************************/
/* ==================================================================== */
/*                       VSIGPOssFilesystemHandler                        */
/* ==================================================================== */
/************************************************************************/

/************************************************************************/
/*                      ~VSIGPOssFilesystemHandler()                      */
/************************************************************************/

VSIGPOssFilesystemHandler::~VSIGPOssFilesystemHandler()

{
    for( const auto &iter : oFileList )
    {
        CPLAtomicDec(&iter.second->nRefCount);
        delete iter.second;
    }

    if( hMutex != nullptr )
        CPLDestroyMutex( hMutex );
    hMutex = nullptr;
}

/************************************************************************/
/*                          CreateFileHandle()                          */
/************************************************************************/

VSIGPOssHandle* VSIGPOssFilesystemHandler::CreateFileHandle(
	                                                const char* pszUnprefixed )
{
    VSIGPOssHandleHelper* poGpOssHandleHelper =
	VSIGPOssHandleHelper::BuildFromURI(pszUnprefixed, GetFSPrefix().c_str(),
		false);
    if( poGpOssHandleHelper )
	return new VSIGPOssHandle(this, pszUnprefixed, poGpOssHandleHelper);
    else
	CPLError(CE_Warning, CPLE_AppDefined,
		"BuildFromURI(%s) failed", pszUnprefixed);
    return NULL;
}

/************************************************************************/
/*                                Open()                                */
/************************************************************************/

VSIVirtualHandle *
VSIGPOssFilesystemHandler::Open( const char *pszFilename,
                               const char *pszAccess,
                               bool bSetError )

{
    if( !STARTS_WITH_CI(pszFilename, GetFSPrefix()) )
	return NULL;

    if( strchr(pszAccess, 'w') != NULL ||
	    strchr(pszAccess, '+') != NULL )
    {
	CPLError(CE_Failure, CPLE_AppDefined,
		"Only read-only mode is supported for /vsigposs");
	return NULL;
    }

    VSIGPOssHandle* poHandle =
	CreateFileHandle(pszFilename + strlen(GetFSPrefix()));
    if( poHandle == NULL )
    {
	return NULL;
    }

    VSIGPOssFile *poFile = nullptr;

    if (oFileList.empty())
    {
	//TODO
	const char *bucket = poHandle->m_poOSSHandleHelper->GetBucket().c_str();
	char *objPrefix = const_cast<char *>(poHandle->m_poOSSHandleHelper->GetObjectKey().c_str());
	if (objPrefix[strlen(objPrefix) - 1] == '/')
	    objPrefix[strlen(objPrefix) - 1] = '\0';
	ossObjectResult *objects = ossListObjects(poHandle->context, bucket, objPrefix);
	if (objects == nullptr || objects == NULL)
	{
	    return NULL;
	}

	for(int i = 0; i < objects->nObjects; i++){
	    VSIGPOssFile *tmpFile = new VSIGPOssFile;
	    CPLString filename = GetFSPrefix() + bucket + "/" + objects->objects[i].key; 
	    tmpFile->osFilename = filename;
	    tmpFile->nMaxLength = objects->objects[i].size;
	    tmpFile->nLength = objects->objects[i].size;
	    CPLAtomicInc(&(tmpFile->nRefCount));
	    oFileList[filename] = tmpFile;
	}
    }
    if( oFileList.find(std::string(pszFilename) + "/") != oFileList.end() )
    {
	poFile = oFileList[std::string(pszFilename) + "/"];
	poFile->bIsDirectory = true;
    }

    if( oFileList.find(pszFilename) != oFileList.end())
    {
	poFile = oFileList[pszFilename];
    }

    if (poFile != nullptr)
    {
	CPLAtomicInc(&(poFile->nRefCount));
	poHandle->poFile = poFile;
	return poHandle;
    }
    return nullptr;
}

/************************************************************************/
/*                                Stat()                                */
/************************************************************************/

int VSIGPOssFilesystemHandler::Stat( const char * pszFilename,
                                   VSIStatBufL * pStatBuf,
                                   int /* nFlags */ )

{
    CPLMutexHolder oHolder( &hMutex );

    const CPLString osFilename = NormalizePath(pszFilename);

    memset( pStatBuf, 0, sizeof(VSIStatBufL) );

    //TODO: if is a directory set pStatBuf.st_mode
    if( osFilename == "/vsigposs/" )
    {
        pStatBuf->st_size = 0;
        pStatBuf->st_mode = S_IFDIR;
        return 0;
    }

    VSIGPOssFile *poFile = nullptr;
    if( oFileList.find(osFilename + "/") != oFileList.end())
    {
	    poFile = oFileList[std::string(pszFilename) + "/"];
    }
    else if (oFileList.find(osFilename) == oFileList.end())
    {
        errno = ENOENT;
        return -1;
    }
    else
    {
	poFile = oFileList[osFilename];
    }

    if (poFile == nullptr)
	poFile = oFileList[osFilename + "/"];

    memset( pStatBuf, 0, sizeof(VSIStatBufL) );

    if( poFile->bIsDirectory )
    {
        pStatBuf->st_size = 0;
        pStatBuf->st_mode = S_IFDIR;
    }
    else
    {
        pStatBuf->st_size = poFile->nLength;
        pStatBuf->st_mode = S_IFREG;
        pStatBuf->st_mtime = poFile->mTime;
    }

    return 0;
}

/************************************************************************/
/*                               Unlink()                               */
/************************************************************************/

int VSIGPOssFilesystemHandler::Unlink( const char * pszFilename )

{
    CPLMutexHolder oHolder( &hMutex );
    return Unlink_unlocked(pszFilename);
}

/************************************************************************/
/*                           Unlink_unlocked()                          */
/************************************************************************/

int VSIGPOssFilesystemHandler::Unlink_unlocked( const char * pszFilename )

{
    const CPLString osFilename = NormalizePath(pszFilename);

    if( oFileList.find(osFilename) == oFileList.end() )
    {
        errno = ENOENT;
        return -1;
    }

    VSIGPOssFile *poFile = oFileList[osFilename];

    if( CPLAtomicDec(&(poFile->nRefCount)) == 0 )
        delete poFile;

    oFileList.erase( oFileList.find(osFilename) );

    return 0;
}

/************************************************************************/
/*                               Mkdir()                                */
/************************************************************************/

int VSIGPOssFilesystemHandler::Mkdir( const char * pszPathname,
                                    long /* nMode */ )

{
    CPLMutexHolder oHolder( &hMutex );

    const CPLString osPathname = NormalizePath(pszPathname);

    if( oFileList.find(osPathname) != oFileList.end() )
    {
        errno = EEXIST;
        return -1;
    }

    VSIGPOssFile *poFile = new VSIGPOssFile;

    poFile->osFilename = osPathname;
    poFile->bIsDirectory = true;
    oFileList[osPathname] = poFile;
    CPLAtomicInc(&(poFile->nRefCount));  // Referenced by file list.

    return 0;
}

/************************************************************************/
/*                               Rmdir()                                */
/************************************************************************/

int VSIGPOssFilesystemHandler::Rmdir( const char * pszPathname )
{
    return Unlink( pszPathname );
}

/************************************************************************/
/*                             ReadDirEx()                              */
/************************************************************************/

char **VSIGPOssFilesystemHandler::ReadDirEx( const char *pszPath,
                                           int nMaxFiles )
{
    CPLMutexHolder oHolder( &hMutex );

    const CPLString osPath = NormalizePath(pszPath);

    char **papszDir = nullptr;
    size_t nPathLen = osPath.size();

    if( nPathLen > 0 && osPath.back() == '/' )
        nPathLen--;

    // In case of really big number of files in the directory, CSLAddString
    // can be slow (see #2158). We then directly build the list.
    int nItems = 0;
    int nAllocatedItems = 0;

    for( const auto& iter : oFileList )
    {
        const char *pszFilePath = iter.second->osFilename.c_str();
        if( EQUALN(osPath, pszFilePath, nPathLen)
            && pszFilePath[nPathLen] == '/'
            && strstr(pszFilePath+nPathLen+1, "/") == nullptr )
        {
            if( nItems == 0 )
            {
                papszDir = static_cast<char**>(CPLCalloc(2, sizeof(char*)));
                nAllocatedItems = 1;
            }
            else if( nItems >= nAllocatedItems )
            {
                nAllocatedItems = nAllocatedItems * 2;
                papszDir = static_cast<char**>(
                    CPLRealloc(papszDir, (nAllocatedItems + 2)*sizeof(char*)) );
            }

            papszDir[nItems] = CPLStrdup(pszFilePath+nPathLen+1);
            papszDir[nItems+1] = nullptr;

            nItems++;
            if( nMaxFiles > 0 && nItems > nMaxFiles )
                break;
        }
    }

    return papszDir;
}

/************************************************************************/
/*                               Rename()                               */
/************************************************************************/

int VSIGPOssFilesystemHandler::Rename( const char *pszOldPath,
                                     const char *pszNewPath )
{
    CPLMutexHolder oHolder( &hMutex );

    const CPLString osOldPath = NormalizePath(pszOldPath);
    const CPLString osNewPath = NormalizePath(pszNewPath);

    if( osOldPath.compare(osNewPath) == 0 )
        return 0;

    if( oFileList.find(osOldPath) == oFileList.end() )
    {
        errno = ENOENT;
        return -1;
    }

    std::map<CPLString, VSIGPOssFile*>::iterator it = oFileList.find(osOldPath);
    while( it != oFileList.end() && it->first.ifind(osOldPath) == 0 )
    {
        const CPLString osRemainder = it->first.substr(osOldPath.size());
        if( osRemainder.empty() || osRemainder[0] == '/' )
        {
            const CPLString osNewFullPath = osNewPath + osRemainder;
            Unlink_unlocked(osNewFullPath);
            oFileList[osNewFullPath] = it->second;
            it->second->osFilename = osNewFullPath;
            oFileList.erase(it++);
        }
        else
        {
            ++it;
        }
    }

    return 0;
}

/************************************************************************/
/*                           NormalizePath()                            */
/************************************************************************/

std::string VSIGPOssFilesystemHandler::NormalizePath( const std::string &in )
{
    std::string s(in);
    std::replace(s.begin(), s.end(), '\\', '/');
    return s;
}

/************************************************************************/
/*                        GetDiskFreeSpace()                            */
/************************************************************************/

GIntBig VSIGPOssFilesystemHandler::GetDiskFreeSpace( const char* /*pszDirname*/ )
{
    const GIntBig nRet = CPLGetUsablePhysicalRAM();
    if( nRet <= 0 )
        return -1;
    return nRet;
}

//! @endcond

/************************************************************************/
/*                       VSIInstallGPOSSFileHandler()                     */
/************************************************************************/

/**
 * \brief Install "memory" file system handler.
 *
 * A special file handler is installed that allows block of memory to be
 * treated as files.   All portions of the file system underneath the base
 * path "/vsimem/" will be handled by this driver.
 *
 * Normal VSI*L functions can be used freely to create and destroy memory
 * arrays treating them as if they were real file system objects.  Some
 * additional methods exist to efficient create memory file system objects
 * without duplicating original copies of the data or to "steal" the block
 * of memory associated with a memory file.
 *
 * Directory related functions are supported.
 *
 * This code example demonstrates using GDAL to translate from one memory
 * buffer to another.
 *
 * \code
 * GByte *ConvertBufferFormat( GByte *pabyInData, vsi_l_offset nInDataLength,
 *                             vsi_l_offset *pnOutDataLength )
 * {
 *     // create memory file system object from buffer.
 *     VSIFCloseL( VSIFileFromMemBuffer( "/vsimem/work.dat", pabyInData,
 *                                       nInDataLength, FALSE ) );
 *
 *     // Open memory buffer for read.
 *     GDALDatasetH hDS = GDALOpen( "/vsimem/work.dat", GA_ReadOnly );
 *
 *     // Get output format driver.
 *     GDALDriverH hDriver = GDALGetDriverByName( "GTiff" );
 *     GDALDatasetH hOutDS;
 *
 *     hOutDS = GDALCreateCopy( hDriver, "/vsimem/out.tif", hDS, TRUE, NULL,
 *                              NULL, NULL );
 *
 *     // close source file, and "unlink" it.
 *     GDALClose( hDS );
 *     VSIUnlink( "/vsimem/work.dat" );
 *
 *     // seize the buffer associated with the output file.
 *
 *     return VSIGetMemFileBuffer( "/vsimem/out.tif", pnOutDataLength, TRUE );
 * }
 * \endcode
 */

void VSIInstallGPOSSFileHandler()
{
    VSIFileManager::InstallHandler( "/vsigposs/", new VSIGPOssFilesystemHandler );
}
