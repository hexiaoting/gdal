/**********************************************************************
 *
 * Name:     cpl_gposs.cpp
 * Project:  CPL - Common Portability Library
 * Purpose:  Amazon Web Services routines
 * Author:   Even Rouault <even.rouault at spatialys.com>
 *
 **********************************************************************
 * Copyright (c) 2015, Even Rouault <even.rouault at spatialys.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

//! @cond Doxygen_Suppress

#include "cpl_gposs.h"
#include "cpl_aws.h"
#include "cpl_vsi_error.h"
#include "cpl_sha256.h"
#include "cpl_time.h"
#include "cpl_minixml.h"
#include "cpl_multiproc.h"
#include "cpl_http.h"
#include <algorithm>

CPL_CVSID("$Id$")

// #define DEBUG_VERBOSE 1

#ifdef HAVE_CURL
static CPLMutex *hMutex = nullptr;
static CPLString osIAMRole;
static CPLString osGlobalAccessKeyId;
static CPLString osGlobalSecretAccessKey;
static CPLString osGlobalSessionToken;
static GIntBig nGlobalExpiration = 0;


/************************************************************************/
/*                         VSIGPOssHandleHelper()                          */
/************************************************************************/
VSIGPOssHandleHelper::VSIGPOssHandleHelper( const CPLString& osSecretAccessKey,
                                      const CPLString& osAccessKeyId,
                                      const CPLString& osSessionToken,
                                      const CPLString& osEndpoint,
                                      const CPLString& osRegion,
                                      const CPLString& osRequestPayer,
                                      const CPLString& osBucket,
                                      const CPLString& osObjectKey
                                      ) :
    //Donot need m_osURL and m_bUseHTTPS
    //m_osURL(BuildURL(osEndpoint, osBucket, osObjectKey,
                   //  bUseVirtualHosting)),
    m_osSecretAccessKey(osSecretAccessKey),
    m_osAccessKeyId(osAccessKeyId),
    m_osSessionToken(osSessionToken),
    m_osEndpoint(osEndpoint),
    m_osRegion(osRegion),
    m_osRequestPayer(osRequestPayer),
    m_osBucket(osBucket),
    m_osObjectKey(osObjectKey)
    //m_bUseHTTPS(bUseHTTPS),
    //m_bUseVirtualHosting(bUseVirtualHosting)
{}

/************************************************************************/
/*                        ~VSIGPOssHandleHelper()                          */
/************************************************************************/

VSIGPOssHandleHelper::~VSIGPOssHandleHelper()
{
    for( size_t i = 0; i < m_osSecretAccessKey.size(); i++ )
        m_osSecretAccessKey[i] = 0;
}


/************************************************************************/
/*                        GetBucketAndObjectKey()                       */
/************************************************************************/

bool VSIGPOssHandleHelper::GetBucketAndObjectKey( const char* pszURI,
                                               const char* pszFSPrefix,
                                               bool bAllowNoObject,
                                               CPLString &osBucket,
                                               CPLString &osObjectKey )
{
    osBucket = pszURI;
    if( osBucket.empty() )
    {
        return false;
    }
    size_t nPos = osBucket.find('/');
    if( nPos == std::string::npos )
    {
        if( bAllowNoObject )
        {
            osObjectKey = "";
            return true;
        }
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Filename should be of the form %sbucket/key", pszFSPrefix);
        return false;
    }
    osBucket.resize(nPos);
    osObjectKey = pszURI + nPos + 1;
    return true;
}


static
void UpdateAndWarnIfInconsistent(const char* pszKeyword,
                                 CPLString& osVal,
                                 const CPLString& osNewVal,
                                 const CPLString& osCredentials,
                                 const CPLString& osConfig)
{
    // nominally defined in ~/.aws/credentials but can
    // be set here too. If both values exist, credentials
    // has the priority
    if( osVal.empty() )
    {
        osVal = osNewVal;
    }
    else if( osVal != osNewVal )
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                    "%s defined in both %s "
                    "and %s. The one of %s will be used",
                    pszKeyword,
                    osCredentials.c_str(),
                    osConfig.c_str(),
                    osCredentials.c_str());
    }
}

/************************************************************************/
/*                GetConfigurationFromAWSConfigFiles()                  */
/************************************************************************/

bool VSIGPOssHandleHelper::GetConfigurationFromAWSConfigFiles(
                                                CPLString& osSecretAccessKey,
                                                CPLString& osAccessKeyId,
                                                CPLString& osSessionToken,
                                                CPLString& osRegion,
                                                CPLString& osCredentials)
{
    // See http://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
    const char* pszProfile = CPLGetConfigOption("AWS_DEFAULT_PROFILE", "");
    const CPLString osProfile(pszProfile[0] != '\0' ? pszProfile : "default");

#ifdef WIN32
    const char* pszHome = CPLGetConfigOption("USERPROFILE", nullptr);
    constexpr char SEP_STRING[] = "\\";
#else
    const char* pszHome = CPLGetConfigOption("HOME", nullptr);
    constexpr char SEP_STRING[] = "/";
#endif

    CPLString osDotAws( pszHome ? pszHome : "" );
    osDotAws += SEP_STRING;
    osDotAws += ".aws";

    // Read first ~/.aws/credential file

    // GDAL specific config option (mostly for testing purpose, but also
    // used in production in some cases)
    const char* pszCredentials =
                    CPLGetConfigOption( "CPL_AWS_CREDENTIALS_FILE", nullptr );
    if( pszCredentials )
    {
        osCredentials = pszCredentials;
    }
    else
    {
        osCredentials = osDotAws;
        osCredentials += SEP_STRING;
        osCredentials += "credentials";
    }
    VSILFILE* fp = VSIFOpenL( osCredentials, "rb" );
    if( fp != nullptr )
    {
        const char* pszLine;
        bool bInProfile = false;
        const CPLString osBracketedProfile("[" + osProfile + "]");
        while( (pszLine = CPLReadLineL(fp)) != nullptr )
        {
            if( pszLine[0] == '[' )
            {
                if( bInProfile )
                    break;
                if( CPLString(pszLine) == osBracketedProfile )
                    bInProfile = true;
            }
            else if( bInProfile )
            {
                char* pszKey = nullptr;
                const char* pszValue = CPLParseNameValue(pszLine, &pszKey);
                if( pszKey && pszValue )
                {
                    if( EQUAL(pszKey, "aws_access_key_id") )
                        osAccessKeyId = pszValue;
                    else if( EQUAL(pszKey, "aws_secret_access_key") )
                        osSecretAccessKey = pszValue;
                    else if( EQUAL(pszKey, "aws_session_token") )
                        osSessionToken = pszValue;
                }
                CPLFree(pszKey);
            }
        }
        VSIFCloseL(fp);
    }

    // And then ~/.aws/config file (unless AWS_CONFIG_FILE is defined)
    const char* pszAWSConfigFileEnv =
                            CPLGetConfigOption( "AWS_CONFIG_FILE", nullptr );
    CPLString osConfig;
    if( pszAWSConfigFileEnv )
    {
        osConfig = pszAWSConfigFileEnv;
    }
    else
    {
        osConfig = osDotAws;
        osConfig += SEP_STRING;
        osConfig += "credentials";
    }
    fp = VSIFOpenL( osConfig, "rb" );
    if( fp != nullptr )
    {
        const char* pszLine;
        bool bInProfile = false;
        const CPLString osBracketedProfile("[" + osProfile + "]");
        const CPLString osBracketedProfileProfile("[profile " + osProfile + "]");
        while( (pszLine = CPLReadLineL(fp)) != nullptr )
        {
            if( pszLine[0] == '[' )
            {
                if( bInProfile )
                    break;
                // In config file, the section name is nominally [profile foo]
                // for the non default profile.
                if( CPLString(pszLine) == osBracketedProfile ||
                    CPLString(pszLine) == osBracketedProfileProfile )
                {
                    bInProfile = true;
                }
            }
            else if( bInProfile )
            {
                char* pszKey = nullptr;
                const char* pszValue = CPLParseNameValue(pszLine, &pszKey);
                if( pszKey && pszValue )
                {
                    if( EQUAL(pszKey, "aws_access_key_id") )
                    {
                        UpdateAndWarnIfInconsistent(pszKey,
                                                    osAccessKeyId,
                                                    pszValue,
                                                    osCredentials,
                                                    osConfig);
                    }
                    else if( EQUAL(pszKey, "aws_secret_access_key") )
                    {
                        UpdateAndWarnIfInconsistent(pszKey,
                                                    osSecretAccessKey,
                                                    pszValue,
                                                    osCredentials,
                                                    osConfig);
                    }
                    else if( EQUAL(pszKey, "aws_session_token") )
                    {
                        UpdateAndWarnIfInconsistent(pszKey,
                                                    osSessionToken,
                                                    pszValue,
                                                    osCredentials,
                                                    osConfig);
                    }
                    else if( EQUAL(pszKey, "region") )
                    {
                        osRegion = pszValue;
                    }
                }
                CPLFree(pszKey);
            }
        }
        VSIFCloseL(fp);
    }
    else if( pszAWSConfigFileEnv != nullptr )
    {
        if( pszAWSConfigFileEnv[0] != '\0' )
        {
            CPLError(CE_Warning, CPLE_AppDefined,
                     "%s does not exist or cannot be open",
                     pszAWSConfigFileEnv);
        }
    }

    return !osAccessKeyId.empty() && !osSecretAccessKey.empty();
}

/************************************************************************/
/*                        GetConfiguration()                            */
/************************************************************************/

bool VSIGPOssHandleHelper::GetConfiguration(CSLConstList papszOptions,
                                         CPLString& osSecretAccessKey,
                                         CPLString& osAccessKeyId,
                                         CPLString& osSessionToken,
                                         CPLString& osRegion)
{
    // AWS_REGION is GDAL specific. Later overloaded by standard
    // AWS_DEFAULT_REGION
    osRegion = CSLFetchNameValueDef(papszOptions, "AWS_REGION",
                            CPLGetConfigOption("AWS_REGION", "ap-northeast-1"));

    if( CPLTestBool(CPLGetConfigOption("AWS_NO_SIGN_REQUEST", "NO")) )
    {
        osSecretAccessKey.clear();
        osAccessKeyId.clear();
        osSessionToken.clear();
        return true;
    }

    osSecretAccessKey = CSLFetchNameValueDef(papszOptions,
        "AWS_SECRET_ACCESS_KEY",
        CPLGetConfigOption("AWS_SECRET_ACCESS_KEY", ""));
    if( !osSecretAccessKey.empty() )
    {
        osAccessKeyId = CPLGetConfigOption("AWS_ACCESS_KEY_ID", "");
        if( osAccessKeyId.empty() )
        {
            VSIError(VSIE_AWSInvalidCredentials,
                    "AWS_ACCESS_KEY_ID configuration option not defined");
            return false;
        }

        osSessionToken = CSLFetchNameValueDef(papszOptions,
            "AWS_SESSION_TOKEN",
            CPLGetConfigOption("AWS_SESSION_TOKEN", ""));
        return true;
    }

    // Next try reading from ~/.aws/credentials and ~/.aws/config
    CPLString osCredentials;
    if( GetConfigurationFromAWSConfigFiles(osSecretAccessKey, osAccessKeyId,
                                           osSessionToken, osRegion,
                                           osCredentials) )
    {
        return true;
    }


    VSIError(VSIE_AWSInvalidCredentials,
                "AWS_SECRET_ACCESS_KEY and AWS_NO_SIGN_REQUEST configuration "
                "options not defined, and %s not filled",
                osCredentials.c_str());
    return false;
}

/************************************************************************/
/*                          CleanMutex()                                */
/************************************************************************/

void VSIGPOssHandleHelper::CleanMutex()
{
    if( hMutex != nullptr )
        CPLDestroyMutex( hMutex );
    hMutex = nullptr;
}

/************************************************************************/
/*                          ClearCache()                                */
/************************************************************************/

void VSIGPOssHandleHelper::ClearCache()
{
    CPLMutexHolder oHolder( &hMutex );

    osIAMRole.clear();
    osGlobalAccessKeyId.clear();
    osGlobalSecretAccessKey.clear();
    osGlobalSessionToken.clear();
    nGlobalExpiration = 0;
}

/************************************************************************/
/*                          BuildFromURI()                              */
/************************************************************************/

VSIGPOssHandleHelper* VSIGPOssHandleHelper::BuildFromURI( const char* pszURI,
                                                    const char* pszFSPrefix,
                                                    bool bAllowNoObject,
                                                    CSLConstList papszOptions )
{
    CPLString osSecretAccessKey;
    CPLString osAccessKeyId;
    CPLString osSessionToken;
    CPLString osRegion;
    if( !GetConfiguration(papszOptions,
                          osSecretAccessKey, osAccessKeyId,
                          osSessionToken, osRegion) )
    {
        return nullptr;
    }

    // According to http://docs.aws.amazon.com/cli/latest/userguide/cli-environment.html
    // " This variable overrides the default region of the in-use profile, if set."
    const CPLString osDefaultRegion = CSLFetchNameValueDef(
        papszOptions, "AWS_DEFAULT_REGION",
        CPLGetConfigOption("AWS_DEFAULT_REGION", ""));
    if( !osDefaultRegion.empty() )
    {
        osRegion = osDefaultRegion;
    }

    const CPLString osEndpoint =
        CPLGetConfigOption("AWS_S3_ENDPOINT", "s3.amazonaws.com");
    const CPLString osRequestPayer =
        CPLGetConfigOption("AWS_REQUEST_PAYER", "");
    CPLString osBucket;
    CPLString osObjectKey;
    //if( pszURI != nullptr && pszURI[0] != '\0' &&
    //    !GetBucketAndObjectKey(pszURI, pszFSPrefix, bAllowNoObject, &osBucket, &osObjectKey) )
    //{
    //    return nullptr;
    //}
    if( pszURI != nullptr && pszURI[0] != '\0') {
	if (GetBucketAndObjectKey(pszURI, pszFSPrefix, bAllowNoObject, osBucket, osObjectKey) == false)
	    return nullptr;
    }
    return new VSIGPOssHandleHelper(osSecretAccessKey, osAccessKeyId,
                                 osSessionToken,
                                 osEndpoint, osRegion,
                                 osRequestPayer,
                                 osBucket, osObjectKey
                                 );
}

/************************************************************************/
/*                          SetEndpoint()                          */
/************************************************************************/

void VSIGPOssHandleHelper::SetEndpoint( const CPLString &osStr )
{
    m_osEndpoint = osStr;
}

/************************************************************************/
/*                           SetRegion()                             */
/************************************************************************/

void VSIGPOssHandleHelper::SetRegion( const CPLString &osStr )
{
    m_osRegion = osStr;
}

/************************************************************************/
/*                           SetRequestPayer()                          */
/************************************************************************/

void VSIGPOssHandleHelper::SetRequestPayer( const CPLString &osStr )
{
    m_osRequestPayer = osStr;
}
#endif

//! @endcond
