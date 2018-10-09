#ifndef CPL_GPOSS_INCLUDED_H
#define CPL_GPOSS_INCLUDED_H

#ifndef DOXYGEN_SKIP

#ifdef HAVE_CURL

#include <cstddef>

#include "cpl_string.h"

#include <map>

class VSIGPOssHandleHelper
{
    CPL_DISALLOW_COPY_ASSIGN(VSIGPOssHandleHelper)

    CPLString m_osAccessKeyId{};
    CPLString m_osSecretAccessKey{};
    CPLString m_osSessionToken{};
    CPLString m_osEndpoint{};
    CPLString m_osRegion{};
    CPLString m_osRequestPayer{};
    CPLString m_osBucket{};
    CPLString m_osObjectKey{};
    CPLString m_ossContext{};

    static bool GetConfigurationFromEC2(CPLString& osSecretAccessKey,
	    CPLString& osAccessKeyId,
	    CPLString& osSessionToken);

    static bool GetConfigurationFromAWSConfigFiles(
	    CPLString& osSecretAccessKey,
	    CPLString& osAccessKeyId,
	    CPLString& osSessionToken,
	    CPLString& osRegion,
	    CPLString& osCredentials);

    static bool GetConfiguration(CSLConstList papszOptions,
	    CPLString& osSecretAccessKey,
	    CPLString& osAccessKeyId,
	    CPLString& osSessionToken,
	    CPLString& osRegion);
    static bool GetBucketAndObjectKey(const char* pszURI,
	    const char* pszFSPrefix,
	    bool bAllowNoObject,
	    CPLString &osBucketOut,
	    CPLString &osObjectKeyOut);
    protected:

    public:
    VSIGPOssHandleHelper(const CPLString& osSecretAccessKey,
	    const CPLString& osAccessKeyId,
	    const CPLString& osSessionToken,
	    const CPLString& osEndpoint,
	    const CPLString& osRegion,
	    const CPLString& osRequestPayer,
	    const CPLString& osBucket,
	    const CPLString& osObjectKey
	    );
    ~VSIGPOssHandleHelper();

    static VSIGPOssHandleHelper* BuildFromURI(const char* pszURI,
	    const char* pszFSPrefix,
	    bool bAllowNoObject,
	    CSLConstList papszOptions = nullptr);

    const CPLString& GetAccessKey() const { return m_osAccessKeyId; }
    const CPLString& GetSecretAccessKey() const { return m_osSecretAccessKey; }
    const CPLString& GetBucket() const { return m_osBucket; }
    const CPLString& GetObjectKey() const { return m_osObjectKey; }
    const CPLString& GetEndpoint()const  { return m_osEndpoint; }
    const CPLString& GetRegion() const { return m_osRegion; }
    const CPLString& GetRequestPayer() const { return m_osRequestPayer; }
    const CPLString& GetContext() const { return m_ossContext; }
    void SetEndpoint(const CPLString &osStr);
    void SetRegion(const CPLString &osStr);
    void SetRequestPayer(const CPLString &osStr);

    CPLString GetSignedURL(CSLConstList papszOptions);

    static void CleanMutex();
    static void ClearCache();
};

#endif /* HAVE_CURL */

#endif /* #ifndef DOXYGEN_SKIP */

#endif /* CPL_AWS_INCLUDED_H */
