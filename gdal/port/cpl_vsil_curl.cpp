/******************************************************************************
 *
 * Project:  CPL - Common Portability Library
 * Purpose:  Implement VSI large file api for HTTP/FTP files
 * Author:   Even Rouault, even.rouault at spatialys.com
 *
 ******************************************************************************
 * Copyright (c) 2010-2018, Even Rouault <even.rouault at spatialys.com>
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

#include "cpl_port.h"
#include "cpl_vsil_curl_priv.h"
#include "cpl_vsil_curl_class.h"

#include <algorithm>
#include <array>
#include <set>
#include <map>
#include <memory>

#include "cpl_aws.h"
// #include "cpl_json.h"
// #include "cpl_json_header.h"
// #include "cpl_minixml.h"
#include "cpl_multiproc.h"
#include "cpl_string.h"
#include "cpl_time.h"
#include "cpl_vsi.h"
#include "cpl_vsi_virtual.h"
#include "cpl_http.h"
// #include "cpl_mem_cache.h"

#ifndef S_IRUSR
#define S_IRUSR     00400
#define S_IWUSR     00200
#define S_IXUSR     00100
#define S_IRGRP     00040
#define S_IWGRP     00020
#define S_IXGRP     00010
#define S_IROTH     00004
#define S_IWOTH     00002
#define S_IXOTH     00001
#endif

CPL_CVSID("$Id$")

#ifndef HAVE_CURL

void VSIInstallCurlFileHandler( void )
{
    // Not supported.
}

void VSICurlClearCache( void )
{
    // Not supported.
}

void VSICurlPartialClearCache(const char* )
{
    // Not supported.
}

void VSICurlAuthParametersChanged()
{
    // Not supported.
}

void VSINetworkStatsReset( void )
{
    // Not supported
}

char *VSINetworkStatsGetAsSerializedJSON( char** /* papszOptions */ )
{
    // Not supported
    return nullptr;
}


/************************************************************************/
/*                      VSICurlInstallReadCbk()                         */
/************************************************************************/

int VSICurlInstallReadCbk ( VSILFILE* /* fp */,
                            VSICurlReadCbkFunc /* pfnReadCbk */,
                            void* /* pfnUserData */,
                            int /* bStopOnInterruptUntilUninstall */)
{
    return FALSE;
}

/************************************************************************/
/*                    VSICurlUninstallReadCbk()                         */
/************************************************************************/

int VSICurlUninstallReadCbk( VSILFILE* /* fp */ )
{
    return FALSE;
}

#else
//! @cond Doxygen_Suppress
#ifndef DOXYGEN_SKIP

#define ENABLE_DEBUG 1
#define ENABLE_DEBUG_VERBOSE 0

/***********************************************************ù************/
/*                    VSICurlAuthParametersChanged()                    */
/************************************************************************/

static unsigned int gnGenerationAuthParameters = 0;

void VSICurlAuthParametersChanged()
{
    gnGenerationAuthParameters++;
}

namespace cpl {

// Do not access those 2 variables directly !
// Use VSICURLGetDownloadChunkSize() and GetMaxRegions()
static int N_MAX_REGIONS_DO_NOT_USE_DIRECTLY = 1000;
static int DOWNLOAD_CHUNK_SIZE_DO_NOT_USE_DIRECTLY = 16384;

/************************************************************************/
/*                    VSICURLReadGlobalEnvVariables()                   */
/************************************************************************/

static void VSICURLReadGlobalEnvVariables()
{
    struct Initializer
    {
        Initializer()
        {
            DOWNLOAD_CHUNK_SIZE_DO_NOT_USE_DIRECTLY = atoi(
                    CPLGetConfigOption("CPL_VSIL_CURL_CHUNK_SIZE", "16384"));
            if( DOWNLOAD_CHUNK_SIZE_DO_NOT_USE_DIRECTLY < 1024 ||
                DOWNLOAD_CHUNK_SIZE_DO_NOT_USE_DIRECTLY > 10 * 1024* 1024 )
                DOWNLOAD_CHUNK_SIZE_DO_NOT_USE_DIRECTLY = 16384;

            GIntBig nCacheSize = CPLAtoGIntBig(
                CPLGetConfigOption("CPL_VSIL_CURL_CACHE_SIZE", "16384000"));
            if( nCacheSize < DOWNLOAD_CHUNK_SIZE_DO_NOT_USE_DIRECTLY ||
                nCacheSize / DOWNLOAD_CHUNK_SIZE_DO_NOT_USE_DIRECTLY > INT_MAX )
            {
                nCacheSize = 16384000;
            }
            N_MAX_REGIONS_DO_NOT_USE_DIRECTLY = std::max(1,
                static_cast<int>(nCacheSize / DOWNLOAD_CHUNK_SIZE_DO_NOT_USE_DIRECTLY));
        }
    };
    static Initializer initializer;
}

/************************************************************************/
/*                     VSICURLGetDownloadChunkSize()                    */
/************************************************************************/

int VSICURLGetDownloadChunkSize()
{
    VSICURLReadGlobalEnvVariables();
    return DOWNLOAD_CHUNK_SIZE_DO_NOT_USE_DIRECTLY;
}

/************************************************************************/
/*                            GetMaxRegions()                           */
/************************************************************************/

static int GetMaxRegions()
{
    VSICURLReadGlobalEnvVariables();
    return N_MAX_REGIONS_DO_NOT_USE_DIRECTLY;
}


/************************************************************************/
/*          VSICurlFindStringSensitiveExceptEscapeSequences()           */
/************************************************************************/

// static int
// VSICurlFindStringSensitiveExceptEscapeSequences( char ** papszList,
//                                                  const char * pszTarget )

// {
//     if( papszList == nullptr )
//         return -1;

//     for( int i = 0; papszList[i] != nullptr; i++ )
//     {
//         const char* pszIter1 = papszList[i];
//         const char* pszIter2 = pszTarget;
//         char ch1 = '\0';
//         char ch2 = '\0';
//         /* The comparison is case-sensitive, escape for escaped */
//         /* sequences where letters of the hexadecimal sequence */
//         /* can be uppercase or lowercase depending on the quoting algorithm */
//         while( true )
//         {
//             ch1 = *pszIter1;
//             ch2 = *pszIter2;
//             if( ch1 == '\0' || ch2 == '\0' )
//                 break;
//             if( ch1 == '%' && ch2 == '%' &&
//                 pszIter1[1] != '\0' && pszIter1[2] != '\0' &&
//                 pszIter2[1] != '\0' && pszIter2[2] != '\0' )
//             {
//                 if( !EQUALN(pszIter1+1, pszIter2+1, 2) )
//                     break;
//                 pszIter1 += 2;
//                 pszIter2 += 2;
//             }
//             if( ch1 != ch2 )
//                 break;
//             pszIter1++;
//             pszIter2++;
//         }
//         if( ch1 == ch2 && ch1 == '\0' )
//             return i;
//     }

//     return -1;
// }

/************************************************************************/
/*                      VSICurlIsFileInList()                           */
/************************************************************************/

// static int VSICurlIsFileInList( char ** papszList, const char * pszTarget )
// {
//     int nRet =
//         VSICurlFindStringSensitiveExceptEscapeSequences(papszList, pszTarget);
//     if( nRet >= 0 )
//         return nRet;

//     // If we didn't find anything, try to URL-escape the target filename.
//     char* pszEscaped = CPLEscapeString(pszTarget, -1, CPLES_URL);
//     if( strcmp(pszTarget, pszEscaped) != 0 )
//     {
//         nRet = VSICurlFindStringSensitiveExceptEscapeSequences(papszList,
//                                                                pszEscaped);
//     }
//     CPLFree(pszEscaped);
//     return nRet;
// }

/************************************************************************/
/*                      VSICurlGetURLFromFilename()                     */
/************************************************************************/

static CPLString VSICurlGetURLFromFilename(const char* pszFilename,
                                           int* pnMaxRetry,
                                           double* pdfRetryDelay,
                                           bool* pbUseHead,
                                           bool* pbListDir,
                                           bool* pbEmptyDir,
                                           char*** ppapszHTTPOptions)
{
    if( !STARTS_WITH(pszFilename, "/vsicurl/") &&
        !STARTS_WITH(pszFilename, "/vsicurl?") )
        return pszFilename;
    pszFilename += strlen("/vsicurl/");
    if( !STARTS_WITH(pszFilename, "http://") &&
        !STARTS_WITH(pszFilename, "https://") &&
        !STARTS_WITH(pszFilename, "ftp://") &&
        !STARTS_WITH(pszFilename, "file://") )
    {
        if( *pszFilename == '?' )
            pszFilename ++;
        char** papszTokens = CSLTokenizeString2( pszFilename, "&", 0 );
        for( int i = 0; papszTokens[i] != nullptr; i++ )
        {
            char* pszUnescaped = CPLUnescapeString( papszTokens[i], nullptr,
                                                    CPLES_URL );
            CPLFree(papszTokens[i]);
            papszTokens[i] = pszUnescaped;
        }

        CPLString osURL;
        for( int i = 0; papszTokens[i]; i++ )
        {
            char* pszKey = nullptr;
            const char* pszValue = CPLParseNameValue(papszTokens[i], &pszKey);
            if( pszKey && pszValue )
            {
                if( EQUAL(pszKey, "max_retry") )
                {
                    if( pnMaxRetry )
                        *pnMaxRetry = atoi(pszValue);
                }
                else if( EQUAL(pszKey, "retry_delay") )
                {
                    if( pdfRetryDelay )
                        *pdfRetryDelay = CPLAtof(pszValue);
                }
                    else if( EQUAL(pszKey, "use_head") )
                {
                    if( pbUseHead )
                        *pbUseHead = CPLTestBool(pszValue);
                }
                else if( EQUAL(pszKey, "list_dir") )
                {
                    if( pbListDir )
                        *pbListDir = CPLTestBool(pszValue);
                }
                else if( EQUAL(pszKey, "empty_dir") )
                {
                    /* Undocumented. Used by PLScenes driver */
                    /* This more or less emulates the behavior of
                        * GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR */
                    if( pbEmptyDir )
                        *pbEmptyDir = CPLTestBool(pszValue);
                }
                else if( EQUAL(pszKey, "useragent") ||
                            EQUAL(pszKey, "referer") ||
                            EQUAL(pszKey, "cookie") ||
                            EQUAL(pszKey, "header_file") ||
                            EQUAL(pszKey, "unsafessl") ||
#ifndef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
                            EQUAL(pszKey, "timeout") ||
                            EQUAL(pszKey, "connecttimeout") ||
#endif
                            EQUAL(pszKey, "low_speed_time") ||
                            EQUAL(pszKey, "low_speed_limit") ||
                            EQUAL(pszKey, "proxy") ||
                            EQUAL(pszKey, "proxyauth") ||
                            EQUAL(pszKey, "proxyuserpwd") )
                {
                    // Above names are the ones supported by
                    // CPLHTTPSetOptions()
                    if( ppapszHTTPOptions )
                    {
                        *ppapszHTTPOptions = CSLSetNameValue(
                            *ppapszHTTPOptions, pszKey, pszValue);
                    }
                }
                else if( EQUAL(pszKey, "url") )
                {
                    osURL = pszValue;
                }
                else
                {
                    CPLError(CE_Warning, CPLE_NotSupported,
                                "Unsupported option: %s", pszKey);
                }
            }
            CPLFree(pszKey);
        }

        CSLDestroy(papszTokens);
        if( osURL.empty() )
        {
            CPLError(CE_Failure, CPLE_IllegalArg, "Missing url parameter");
            return pszFilename;
        }

        return osURL;
    }

    return pszFilename;
}

/************************************************************************/
/*                           VSICurlHandle()                            */
/************************************************************************/

VSICurlHandle::VSICurlHandle( VSICurlFilesystemHandler* poFSIn,
                              const char* pszFilename,
                              const char* pszURLIn ) :
    poFS(poFSIn),
    m_osFilename(pszFilename),
    m_nMaxRetry(atoi(CPLGetConfigOption("GDAL_HTTP_MAX_RETRY",
                                   CPLSPrintf("%d",CPL_HTTP_MAX_RETRY)))),
    // coverity[tainted_data]
    m_dfRetryDelay(CPLAtof(CPLGetConfigOption("GDAL_HTTP_RETRY_DELAY",
                                CPLSPrintf("%f", CPL_HTTP_RETRY_DELAY)))),
    m_bUseHead(CPLTestBool(CPLGetConfigOption("CPL_VSIL_CURL_USE_HEAD",
                                             "YES")))
{
    m_papszHTTPOptions = CPLHTTPGetOptionsFromEnv();
    if( pszURLIn )
    {
        m_pszURL = CPLStrdup(pszURLIn);
    }
    else
    {
        m_pszURL = CPLStrdup(VSICurlGetURLFromFilename(pszFilename,
                                                       &m_nMaxRetry,
                                                       &m_dfRetryDelay,
                                                       &m_bUseHead,
                                                       nullptr, nullptr,
                                                       &m_papszHTTPOptions));
    }

    m_bCached = poFSIn->AllowCachedDataFor(pszFilename);
    poFS->GetCachedFileProp(m_pszURL, oFileProp);
}

/************************************************************************/
/*                          ~VSICurlHandle()                            */
/************************************************************************/

VSICurlHandle::~VSICurlHandle()
{
    if( !m_bCached )
    {
        poFS->InvalidateCachedData(m_pszURL);
        // poFS->InvalidateDirContent( CPLGetDirname(m_osFilename) );
    }
    CPLFree(m_pszURL);
    CSLDestroy(m_papszHTTPOptions);
}

/************************************************************************/
/*                            SetURL()                                  */
/************************************************************************/

void VSICurlHandle::SetURL(const char* pszURLIn)
{
    CPLFree(m_pszURL);
    m_pszURL = CPLStrdup(pszURLIn);
}

/************************************************************************/
/*                          InstallReadCbk()                            */
/************************************************************************/

int VSICurlHandle::InstallReadCbk( VSICurlReadCbkFunc pfnReadCbkIn,
                                   void* pfnUserDataIn,
                                   int bStopOnInterruptUntilUninstallIn )
{
    if( pfnReadCbk != nullptr )
        return FALSE;

    pfnReadCbk = pfnReadCbkIn;
    pReadCbkUserData = pfnUserDataIn;
    bStopOnInterruptUntilUninstall =
        CPL_TO_BOOL(bStopOnInterruptUntilUninstallIn);
    bInterrupted = false;
    return TRUE;
}

/************************************************************************/
/*                         UninstallReadCbk()                           */
/************************************************************************/

int VSICurlHandle::UninstallReadCbk()
{
    if( pfnReadCbk == nullptr )
        return FALSE;

    pfnReadCbk = nullptr;
    pReadCbkUserData = nullptr;
    bStopOnInterruptUntilUninstall = false;
    bInterrupted = false;
    return TRUE;
}

/************************************************************************/
/*                                Seek()                                */
/************************************************************************/

int VSICurlHandle::Seek( vsi_l_offset nOffset, int nWhence )
{
    if( nWhence == SEEK_SET )
    {
        curOffset = nOffset;
    }
    else if( nWhence == SEEK_CUR )
    {
        curOffset = curOffset + nOffset;
    }
    else
    {
        curOffset = GetFileSize(false) + nOffset;
    }
    bEOF = false;
    return 0;
}

/************************************************************************/
/*                 VSICurlGetTimeStampFromRFC822DateTime()              */
/************************************************************************/

static GIntBig VSICurlGetTimeStampFromRFC822DateTime( const char* pszDT )
{
    // Sun, 03 Apr 2016 12:07:27 GMT
    if( strlen(pszDT) >= 5 && pszDT[3] == ',' && pszDT[4] == ' ' )
        pszDT += 5;
    int nDay = 0;
    int nYear = 0;
    int nHour = 0;
    int nMinute = 0;
    int nSecond = 0;
    char szMonth[4] = {};
    szMonth[3] = 0;
    if( sscanf(pszDT, "%02d %03s %04d %02d:%02d:%02d GMT",
                &nDay, szMonth, &nYear, &nHour, &nMinute, &nSecond) == 6 )
    {
        static const char* const aszMonthStr[] = {
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

        int nMonthIdx0 = -1;
        for( int i = 0; i < 12; i++ )
        {
            if( EQUAL(szMonth, aszMonthStr[i]) )
            {
                nMonthIdx0 = i;
                break;
            }
        }
        if( nMonthIdx0 >= 0 )
        {
            struct tm brokendowntime;
            brokendowntime.tm_year = nYear - 1900;
            brokendowntime.tm_mon = nMonthIdx0;
            brokendowntime.tm_mday = nDay;
            brokendowntime.tm_hour = nHour;
            brokendowntime.tm_min = nMinute;
            brokendowntime.tm_sec = nSecond;
            return CPLYMDHMSToUnixTime(&brokendowntime);
        }
    }
    return 0;
}

/************************************************************************/
/*                    VSICURLInitWriteFuncStruct()                      */
/************************************************************************/

void VSICURLInitWriteFuncStruct( WriteFuncStruct   *psStruct,
                                        VSILFILE          *fp,
                                        VSICurlReadCbkFunc pfnReadCbk,
                                        void              *pReadCbkUserData )
{
    psStruct->pBuffer = nullptr;
    psStruct->nSize = 0;
    psStruct->bIsHTTP = false;
    psStruct->bIsInHeader = true;
    psStruct->bMultiRange = false;
    psStruct->nStartOffset = 0;
    psStruct->nEndOffset = 0;
    psStruct->nHTTPCode = 0;
    psStruct->nContentLength = 0;
    psStruct->bFoundContentRange = false;
    psStruct->bError = false;
    psStruct->bDownloadHeaderOnly = false;
    psStruct->bDetectRangeDownloadingError = true;
    psStruct->nTimestampDate = 0;

    psStruct->fp = fp;
    psStruct->pfnReadCbk = pfnReadCbk;
    psStruct->pReadCbkUserData = pReadCbkUserData;
    psStruct->bInterrupted = false;

#if !CURL_AT_LEAST_VERSION(7,54,0)
    psStruct->bIsProxyConnectHeader = false;
#endif //!CURL_AT_LEAST_VERSION(7,54,0)
}

/************************************************************************/
/*                       VSICurlHandleWriteFunc()                       */
/************************************************************************/

size_t VSICurlHandleWriteFunc( void *buffer, size_t count,
                                      size_t nmemb, void *req )
{
    WriteFuncStruct* psStruct = static_cast<WriteFuncStruct *>(req);
    const size_t nSize = count * nmemb;

    char* pNewBuffer = static_cast<char *>(
        VSIRealloc(psStruct->pBuffer, psStruct->nSize + nSize + 1));
    if( pNewBuffer )
    {
        psStruct->pBuffer = pNewBuffer;
        memcpy(psStruct->pBuffer + psStruct->nSize, buffer, nSize);
        psStruct->pBuffer[psStruct->nSize + nSize] = '\0';
        if( psStruct->bIsHTTP && psStruct->bIsInHeader )
        {
            char* pszLine = psStruct->pBuffer + psStruct->nSize;
            if( STARTS_WITH_CI(pszLine, "HTTP/") )
            {
                char* pszSpace = strchr(pszLine, ' ');
                if( pszSpace )
                {
                    psStruct->nHTTPCode = atoi(pszSpace + 1);

#if !CURL_AT_LEAST_VERSION(7,54,0)
                    // Workaround to ignore extra HTTP response headers from
                    // proxies in older versions of curl.
                    // CURLOPT_SUPPRESS_CONNECT_HEADERS fixes this
                    if( psStruct->nHTTPCode >= 200 &&
                        psStruct->nHTTPCode < 300 )
                    {
                        pszSpace = strchr(pszSpace + 1, ' ');
                        if( pszSpace &&
                            // This could be any string really, but we don't
                            // have an easy way to distinguish between proxies
                            // and upstream responses...
                            STARTS_WITH_CI( pszSpace + 1,
                                            "Connection established") )
                        {
                            psStruct->bIsProxyConnectHeader = true;
                        }
                    }
#endif //!CURL_AT_LEAST_VERSION(7,54,0)
                }
            }
            else if( STARTS_WITH_CI(pszLine, "Content-Length: ") )
            {
                psStruct->nContentLength =
                    CPLScanUIntBig(pszLine + 16,
                                   static_cast<int>(strlen(pszLine + 16)));
            }
            else if( STARTS_WITH_CI(pszLine, "Content-Range: ") )
            {
                psStruct->bFoundContentRange = true;
            }
            else if( STARTS_WITH_CI(pszLine, "Date: ") )
            {
                CPLString osDate = pszLine + strlen("Date: ");
                size_t nSizeLine = osDate.size();
                while( nSizeLine &&
                       (osDate[nSizeLine-1] == '\r' ||
                        osDate[nSizeLine-1] == '\n') )
                {
                    osDate.resize(nSizeLine-1);
                    nSizeLine--;
                }
                osDate.Trim();

                GIntBig nTimestampDate =
                    VSICurlGetTimeStampFromRFC822DateTime(osDate);
#if DEBUG_VERBOSE
                CPLDebug("VSICURL",
                         "Timestamp = " CPL_FRMT_GIB, nTimestampDate);
#endif
                psStruct->nTimestampDate = nTimestampDate;
            }
            /*if( nSize > 2 && pszLine[nSize - 2] == '\r' &&
                  pszLine[nSize - 1] == '\n' )
            {
                pszLine[nSize - 2] = 0;
                CPLDebug("VSICURL", "%s", pszLine);
                pszLine[nSize - 2] = '\r';
            }*/

            if( pszLine[0] == '\r' || pszLine[0] == '\n' )
            {
                if( psStruct->bDownloadHeaderOnly )
                {
                    // If moved permanently/temporarily, go on.
                    // Otherwise stop now,
                    if( !(psStruct->nHTTPCode == 301 ||
                          psStruct->nHTTPCode == 302) )
                        return 0;
                }
#if !CURL_AT_LEAST_VERSION(7,54,0)
                else if( psStruct->bIsProxyConnectHeader )
                {
                    psStruct->bIsProxyConnectHeader = false;
                }
#endif //!CURL_AT_LEAST_VERSION(7,54,0)
                else
                {
                    psStruct->bIsInHeader = false;

                    // Detect servers that don't support range downloading.
                    if( psStruct->nHTTPCode == 200 &&
                        psStruct->bDetectRangeDownloadingError &&
                        !psStruct->bMultiRange &&
                        !psStruct->bFoundContentRange &&
                        (psStruct->nStartOffset != 0 ||
                         psStruct->nContentLength > 10 *
                         (psStruct->nEndOffset - psStruct->nStartOffset + 1)) )
                    {
                        CPLError(CE_Failure, CPLE_AppDefined,
                                 "Range downloading not supported by this "
                                 "server!");
                        psStruct->bError = true;
                        return 0;
                    }
                }
            }
        }
        else
        {
            if( psStruct->pfnReadCbk )
            {
                if( !psStruct->pfnReadCbk(psStruct->fp, buffer, nSize,
                                          psStruct->pReadCbkUserData) )
                {
                    psStruct->bInterrupted = true;
                    return 0;
                }
            }
        }
        psStruct->nSize += nSize;
        return nmemb;
    }
    else
    {
        return 0;
    }
}

/************************************************************************/
/*                    VSICurlIsS3LikeSignedURL()                        */
/************************************************************************/

static bool VSICurlIsS3LikeSignedURL( const char* pszURL )
{
    return
        ((strstr(pszURL, ".s3.amazonaws.com/") != nullptr ||
          strstr(pszURL, ".s3.amazonaws.com:") != nullptr ||
          strstr(pszURL, ".storage.googleapis.com/") != nullptr ||
          strstr(pszURL, ".storage.googleapis.com:") != nullptr) &&
         (strstr(pszURL, "&Signature=") != nullptr ||
          strstr(pszURL, "?Signature=") != nullptr)) ||
        strstr(pszURL, "&X-Amz-Signature=") != nullptr ||
        strstr(pszURL, "?X-Amz-Signature=") != nullptr;
}

/************************************************************************/
/*                  VSICurlGetExpiresFromS3LikeSignedURL()              */
/************************************************************************/

static GIntBig VSICurlGetExpiresFromS3LikeSignedURL( const char* pszURL )
{
    const auto GetParamValue = [pszURL](const char* pszKey) -> const char*
    {
        for( const char* pszPrefix: { "&", "?" } )
        {
            std::string osNeedle(pszPrefix);
            osNeedle += pszKey;
            osNeedle += '=';
            const char* pszStr = strstr(pszURL, osNeedle.c_str());
            if( pszStr )
                return pszStr + osNeedle.size();
        }
        return nullptr;
    };

    {
        // Expires= is a Unix timestamp
        const char* pszExpires = GetParamValue("Expires");
        if( pszExpires != nullptr )
            return CPLAtoGIntBig(pszExpires);
    }

    // X-Amz-Expires= is a delay, to be combined with X-Amz-Date=
    const char* pszAmzExpires = GetParamValue("X-Amz-Expires");
    if( pszAmzExpires == nullptr )
        return 0;
    const int nDelay = atoi(pszAmzExpires);

    const char* pszAmzDate = GetParamValue("X-Amz-Date");
    if( pszAmzDate == nullptr )
        return 0;
    // pszAmzDate should be YYYYMMDDTHHMMSSZ
    if( strlen(pszAmzDate) < strlen("YYYYMMDDTHHMMSSZ") )
        return 0;
    if( pszAmzDate[strlen("YYYYMMDDTHHMMSSZ")-1] != 'Z' )
        return 0;
    struct tm brokendowntime;
    brokendowntime.tm_year = atoi(std::string(pszAmzDate).substr(0, 4).c_str()) - 1900;
    brokendowntime.tm_mon = atoi(std::string(pszAmzDate).substr(4, 2).c_str()) - 1;
    brokendowntime.tm_mday = atoi(std::string(pszAmzDate).substr(6, 2).c_str());
    brokendowntime.tm_hour = atoi(std::string(pszAmzDate).substr(9, 2).c_str());
    brokendowntime.tm_min = atoi(std::string(pszAmzDate).substr(11, 2).c_str());
    brokendowntime.tm_sec = atoi(std::string(pszAmzDate).substr(13, 2).c_str());
    return CPLYMDHMSToUnixTime(&brokendowntime) + nDelay;
}

/************************************************************************/
/*                           MultiPerform()                             */
/************************************************************************/

void MultiPerform(CURLM* hCurlMultiHandle, CURL* hEasyHandle)
{
    int repeats = 0;

    if( hEasyHandle )
        curl_multi_add_handle(hCurlMultiHandle, hEasyHandle);

    void* old_handler = CPLHTTPIgnoreSigPipe();
    while( true )
    {
        int still_running;
        while (curl_multi_perform(hCurlMultiHandle, &still_running) ==
                                        CURLM_CALL_MULTI_PERFORM )
        {
            // loop
        }
        if( !still_running )
        {
            break;
        }

#ifdef undef
        CURLMsg *msg;
        do {
            int msgq = 0;
            msg = curl_multi_info_read(hCurlMultiHandle, &msgq);
            if(msg && (msg->msg == CURLMSG_DONE))
            {
                CURL *e = msg->easy_handle;
            }
        } while(msg);
#endif

        CPLMultiPerformWait(hCurlMultiHandle, repeats);
    }
    CPLHTTPRestoreSigPipeHandler(old_handler);

    if( hEasyHandle )
        curl_multi_remove_handle(hCurlMultiHandle, hEasyHandle);
}

/************************************************************************/
/*                       VSICurlDummyWriteFunc()                        */
/************************************************************************/

static size_t VSICurlDummyWriteFunc( void *, size_t , size_t , void * )
{
    return 0;
}

/************************************************************************/
/*                  VSICURLResetHeaderAndWriterFunctions()              */
/************************************************************************/

void VSICURLResetHeaderAndWriterFunctions(CURL* hCurlHandle)
{
    curl_easy_setopt(hCurlHandle, CURLOPT_HEADERFUNCTION,
                     VSICurlDummyWriteFunc);
    curl_easy_setopt(hCurlHandle, CURLOPT_WRITEFUNCTION,
                     VSICurlDummyWriteFunc);
}

/************************************************************************/
/*                     GetFileSizeOrHeaders()                           */
/************************************************************************/

vsi_l_offset VSICurlHandle::GetFileSizeOrHeaders( bool bSetError, bool bGetHeaders )
{
    if( oFileProp.bHasComputedFileSize && !bGetHeaders )
        return oFileProp.fileSize;

    // NetworkStatisticsFileSystem oContextFS(poFS->GetFSPrefix());
    // NetworkStatisticsFile oContextFile(m_osFilename);
    // NetworkStatisticsAction oContextAction("GetFileSize");

    oFileProp.bHasComputedFileSize = true;

    CURLM* hCurlMultiHandle = poFS->GetCurlMultiHandleFor(m_pszURL);

    CPLString osURL(m_pszURL + m_osQueryString);
    bool bRetryWithGet = false;
    bool bS3LikeRedirect = false;
    int nRetryCount = 0;
    double dfRetryDelay = m_dfRetryDelay;

retry:
    CURL* hCurlHandle = curl_easy_init();

    struct curl_slist* headers =
            VSICurlSetOptions(hCurlHandle, osURL, m_papszHTTPOptions);

    WriteFuncStruct sWriteFuncHeaderData;
    VSICURLInitWriteFuncStruct(&sWriteFuncHeaderData, nullptr, nullptr, nullptr);

    CPLString osVerb;
    CPLString osRange; // leave in this scope !
    int nRoundedBufSize = 0;
    const int knDOWNLOAD_CHUNK_SIZE = VSICURLGetDownloadChunkSize();
    if( UseLimitRangeGetInsteadOfHead() )
    {
        osVerb = "GET";
        const int nBufSize = std::max(1024, std::min(10 * 1024 * 1024,
            atoi(CPLGetConfigOption("GDAL_INGESTED_BYTES_AT_OPEN", "1024"))));
        nRoundedBufSize = ((nBufSize + knDOWNLOAD_CHUNK_SIZE - 1)
            / knDOWNLOAD_CHUNK_SIZE) * knDOWNLOAD_CHUNK_SIZE;

        // so it gets included in Azure signature
        osRange.Printf("Range: bytes=0-%d", nRoundedBufSize-1);
        headers = curl_slist_append(headers, osRange.c_str());
        sWriteFuncHeaderData.bDetectRangeDownloadingError = false;
    }
    // HACK for mbtiles driver: http://a.tiles.mapbox.com/v3/ doesn't accept
    // HEAD, as it is a redirect to AWS S3 signed URL, but those are only valid
    // for a given type of HTTP request, and thus GET. This is valid for any
    // signed URL for AWS S3.
    else if( bRetryWithGet ||
             strstr(osURL, ".tiles.mapbox.com/") != nullptr ||
             VSICurlIsS3LikeSignedURL(osURL) ||
             !m_bUseHead )
    {
        sWriteFuncHeaderData.bDownloadHeaderOnly = true;
        osVerb = "GET";
    }
    else
    {
        sWriteFuncHeaderData.bDetectRangeDownloadingError = false;
        curl_easy_setopt(hCurlHandle, CURLOPT_NOBODY, 1);
        curl_easy_setopt(hCurlHandle, CURLOPT_HTTPGET, 0);
        curl_easy_setopt(hCurlHandle, CURLOPT_HEADER, 1);
        osVerb = "HEAD";
    }

    if( !AllowAutomaticRedirection() )
        curl_easy_setopt(hCurlHandle, CURLOPT_FOLLOWLOCATION, 0);

    curl_easy_setopt(hCurlHandle, CURLOPT_HEADERDATA, &sWriteFuncHeaderData);
    curl_easy_setopt(hCurlHandle, CURLOPT_HEADERFUNCTION,
                     VSICurlHandleWriteFunc);
    sWriteFuncHeaderData.bIsHTTP = STARTS_WITH(osURL, "http");

    // Bug with older curl versions (<=7.16.4) and FTP.
    // See http://curl.haxx.se/mail/lib-2007-08/0312.html
    WriteFuncStruct sWriteFuncData;
    VSICURLInitWriteFuncStruct(&sWriteFuncData, nullptr, nullptr, nullptr);
    curl_easy_setopt(hCurlHandle, CURLOPT_WRITEDATA, &sWriteFuncData);
    curl_easy_setopt(hCurlHandle, CURLOPT_WRITEFUNCTION,
                     VSICurlHandleWriteFunc);

    char szCurlErrBuf[CURL_ERROR_SIZE+1] = {};
    szCurlErrBuf[0] = '\0';
    curl_easy_setopt(hCurlHandle, CURLOPT_ERRORBUFFER, szCurlErrBuf );

    headers = VSICurlMergeHeaders(headers, GetCurlHeaders(osVerb, headers));
    curl_easy_setopt(hCurlHandle, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(hCurlHandle, CURLOPT_FILETIME, 1);

    MultiPerform(hCurlMultiHandle, hCurlHandle);

    VSICURLResetHeaderAndWriterFunctions(hCurlHandle);

    curl_slist_free_all(headers);

    oFileProp.eExists = EXIST_UNKNOWN;

    long mtime = 0;
    curl_easy_getinfo(hCurlHandle, CURLINFO_FILETIME, &mtime);

    // if( osVerb == "GET" )
    //     NetworkStatisticsLogger::LogGET(sWriteFuncData.nSize);
    // else
    //     NetworkStatisticsLogger::LogHEAD();

    if( STARTS_WITH(osURL, "ftp") )
    {
        if( sWriteFuncData.pBuffer != nullptr )
        {
            const char* pszContentLength = strstr(
                const_cast<const char*>(sWriteFuncData.pBuffer), "Content-Length: ");
            if( pszContentLength )
            {
                pszContentLength += strlen("Content-Length: ");
                oFileProp.eExists = EXIST_YES;
                oFileProp.fileSize = CPLScanUIntBig(
                    pszContentLength,
                    static_cast<int>(strlen(pszContentLength)));
                if( ENABLE_DEBUG )
                    CPLDebug(poFS->GetDebugKey(),
                             "GetFileSize(%s)=" CPL_FRMT_GUIB,
                            osURL.c_str(), oFileProp.fileSize);
            }
        }
    }

    if( ENABLE_DEBUG && szCurlErrBuf[0] != '\0' &&
        sWriteFuncHeaderData.bDownloadHeaderOnly &&
        EQUAL(szCurlErrBuf, "Failed writing header") )
    {
        // Not really an error since we voluntarily interrupted the download !
        szCurlErrBuf[0] = 0;
    }

    double dfSize = 0;
    if( oFileProp.eExists != EXIST_YES )
    {
        long response_code = 0;
        curl_easy_getinfo(hCurlHandle, CURLINFO_HTTP_CODE, &response_code);

        if( ENABLE_DEBUG && szCurlErrBuf[0] != '\0' )
        {
            CPLDebug(poFS->GetDebugKey(),
                     "GetFileSize(%s): response_code=%d, msg=%s",
                     osURL.c_str(),
                     static_cast<int>(response_code),
                     szCurlErrBuf);
        }

        CPLString osEffectiveURL;
        {
            char *pszEffectiveURL = nullptr;
            curl_easy_getinfo(hCurlHandle, CURLINFO_EFFECTIVE_URL,
                              &pszEffectiveURL);
            if( pszEffectiveURL )
                osEffectiveURL = pszEffectiveURL;
        }

        if( !osEffectiveURL.empty() && strstr(osEffectiveURL, osURL) == nullptr )
        {
            CPLDebug(poFS->GetDebugKey(),
                     "Effective URL: %s", osEffectiveURL.c_str());

            // Is this is a redirect to a S3 URL?
            if( VSICurlIsS3LikeSignedURL(osEffectiveURL) &&
                !VSICurlIsS3LikeSignedURL(osURL) )
            {
                // Note that this is a redirect as we won't notice after the
                // retry.
                bS3LikeRedirect = true;

                if( !bRetryWithGet && osVerb == "HEAD" && response_code == 403 )
                {
                    CPLDebug(poFS->GetDebugKey(),
                             "Redirected to a AWS S3 signed URL. Retrying "
                             "with GET request instead of HEAD since the URL "
                             "might be valid only for GET");
                    bRetryWithGet = true;
                    osURL = osEffectiveURL;
                    CPLFree(sWriteFuncData.pBuffer);
                    CPLFree(sWriteFuncHeaderData.pBuffer);
                    curl_easy_cleanup(hCurlHandle);
                    goto retry;
                }
            }
        }

        if( bS3LikeRedirect && response_code >= 200 && response_code < 300 &&
            sWriteFuncHeaderData.nTimestampDate > 0 &&
            !osEffectiveURL.empty() &&
            CPLTestBool(CPLGetConfigOption("CPL_VSIL_CURL_USE_S3_REDIRECT",
                                           "TRUE")) )
        {
            const GIntBig nExpireTimestamp =
                VSICurlGetExpiresFromS3LikeSignedURL(osEffectiveURL);
            if( nExpireTimestamp > sWriteFuncHeaderData.nTimestampDate + 10 )
            {
                const int nValidity =
                    static_cast<int>(nExpireTimestamp -
                                     sWriteFuncHeaderData.nTimestampDate);
                CPLDebug(poFS->GetDebugKey(),
                         "Will use redirect URL for the next %d seconds",
                         nValidity);
                // As our local clock might not be in sync with server clock,
                // figure out the expiration timestamp in local time
                oFileProp.bS3LikeRedirect = true;
                oFileProp.nExpireTimestampLocal = time(nullptr) + nValidity;
                oFileProp.osRedirectURL = osEffectiveURL;
                poFS->SetCachedFileProp(m_pszURL, oFileProp);
            }
        }

        const CURLcode code =
            curl_easy_getinfo(hCurlHandle, CURLINFO_CONTENT_LENGTH_DOWNLOAD,
                              &dfSize );
        if( code == 0 )
        {
            oFileProp.eExists = EXIST_YES;
            if( dfSize < 0 )
            {
                if( osVerb == "HEAD" && !bRetryWithGet && response_code == 200 )
                {
                    CPLDebug(poFS->GetDebugKey(),
                             "HEAD did not provide file size. Retrying with GET");
                    bRetryWithGet = true;
                    CPLFree(sWriteFuncData.pBuffer);
                    CPLFree(sWriteFuncHeaderData.pBuffer);
                    curl_easy_cleanup(hCurlHandle);
                    goto retry;
                }
                oFileProp.fileSize = 0;
            }
            else
                oFileProp.fileSize = static_cast<GUIntBig>(dfSize);
        }

        if( sWriteFuncHeaderData.pBuffer != nullptr &&
            (response_code == 200 || response_code == 206 ) )
        {
            const char* pzETag = strstr(
                sWriteFuncHeaderData.pBuffer, "ETag: \"");
            if( pzETag )
            {
                pzETag += strlen("ETag: \"");
                const char* pszEndOfETag = strchr(pzETag, '"');
                if( pszEndOfETag )
                {
                    oFileProp.ETag.assign(pzETag, pszEndOfETag - pzETag);
                }
            }

            // Azure Data Lake Storage
            const char* pszPermissions = strstr(sWriteFuncHeaderData.pBuffer, "x-ms-permissions: ");
            if( pszPermissions )
            {
                pszPermissions += strlen("x-ms-permissions: ");
                const char* pszEOL = strstr(pszPermissions, "\r\n");
                if( pszEOL )
                {
                    bool bIsDir = strstr(sWriteFuncHeaderData.pBuffer, "x-ms-resource-type: directory\r\n") != nullptr;
                    bool bIsFile = strstr(sWriteFuncHeaderData.pBuffer, "x-ms-resource-type: file\r\n") != nullptr;
                    if( bIsDir || bIsFile )
                    {
                        oFileProp.bIsDirectory = bIsDir;
                        CPLString osPermissions;
                        osPermissions.assign(pszPermissions,
                                            pszEOL - pszPermissions);
                        if( bIsDir )
                            oFileProp.nMode = S_IFDIR;
                        else
                            oFileProp.nMode = S_IFREG;
                        oFileProp.nMode |= VSICurlParseUnixPermissions(osPermissions);
                    }
                }
            }

            if( bGetHeaders )
            {
                char** papszHeaders = CSLTokenizeString2(sWriteFuncHeaderData.pBuffer, "\r\n", 0);
                for( int i = 0; papszHeaders[i]; ++i )
                {
                    char* pszKey = nullptr;
                    const char* pszValue = CPLParseNameValue(papszHeaders[i], &pszKey);
                    if( pszKey && pszValue )
                    {
                        m_aosHeaders.SetNameValue(pszKey, pszValue);
                    }
                    CPLFree(pszKey);
                }
                CSLDestroy(papszHeaders);
            }
        }

        if( UseLimitRangeGetInsteadOfHead() && response_code == 206 )
        {
            oFileProp.eExists = EXIST_NO;
            oFileProp.fileSize = 0;
            if( sWriteFuncHeaderData.pBuffer != nullptr )
            {
                const char* pszContentRange =
                    strstr(sWriteFuncHeaderData.pBuffer,
                           "Content-Range: bytes ");
                if( pszContentRange == nullptr )
                    pszContentRange = strstr(sWriteFuncHeaderData.pBuffer,
                           "content-range: bytes ");
                if( pszContentRange )
                    pszContentRange = strchr(pszContentRange, '/');
                if( pszContentRange )
                {
                    oFileProp.eExists = EXIST_YES;
                    oFileProp.fileSize = static_cast<GUIntBig>(
                        CPLAtoGIntBig(pszContentRange + 1));
                }

                // Add first bytes to cache
                if( sWriteFuncData.pBuffer != nullptr )
                {
                    for( size_t nOffset = 0;
                            nOffset + knDOWNLOAD_CHUNK_SIZE <= sWriteFuncData.nSize;
                            nOffset += knDOWNLOAD_CHUNK_SIZE )
                    {
                        poFS->AddRegion(m_pszURL,
                                        nOffset,
                                        knDOWNLOAD_CHUNK_SIZE,
                                        sWriteFuncData.pBuffer + nOffset);
                    }
                }
            }
        }
        else if ( IsDirectoryFromExists(osVerb,
                                        static_cast<int>(response_code)) )
        {
            oFileProp.eExists = EXIST_YES;
            oFileProp.fileSize = 0;
            oFileProp.bIsDirectory = true;
        }
        // 405 = Method not allowed
        else if (response_code == 405 && !bRetryWithGet && osVerb == "HEAD" )
        {
            CPLDebug(poFS->GetDebugKey(), "HEAD not allowed. Retrying with GET");
            bRetryWithGet = true;
            CPLFree(sWriteFuncData.pBuffer);
            CPLFree(sWriteFuncHeaderData.pBuffer);
            curl_easy_cleanup(hCurlHandle);
            goto retry;
        }
        else if( response_code == 416 )
        {
            oFileProp.eExists = EXIST_YES;
            oFileProp.fileSize = 0;
        }
        else if( response_code != 200 )
        {
            // Look if we should attempt a retry
            const double dfNewRetryDelay = CPLHTTPGetNewRetryDelay(
                static_cast<int>(response_code), dfRetryDelay,
                sWriteFuncHeaderData.pBuffer, szCurlErrBuf);
            if( dfNewRetryDelay > 0 &&
                nRetryCount < m_nMaxRetry )
            {
                CPLError(CE_Warning, CPLE_AppDefined,
                            "HTTP error code: %d - %s. "
                            "Retrying again in %.1f secs",
                            static_cast<int>(response_code), m_pszURL,
                            dfRetryDelay);
                CPLSleep(dfRetryDelay);
                dfRetryDelay = dfNewRetryDelay;
                nRetryCount++;
                CPLFree(sWriteFuncData.pBuffer);
                CPLFree(sWriteFuncHeaderData.pBuffer);
                curl_easy_cleanup(hCurlHandle);
                goto retry;
            }

            if( UseLimitRangeGetInsteadOfHead() &&
                sWriteFuncData.pBuffer != nullptr &&
                CanRestartOnError(sWriteFuncData.pBuffer,
                                  sWriteFuncHeaderData.pBuffer,
                                  bSetError) )
            {
                oFileProp.bHasComputedFileSize = false;
                CPLFree(sWriteFuncData.pBuffer);
                CPLFree(sWriteFuncHeaderData.pBuffer);
                curl_easy_cleanup(hCurlHandle);
                return GetFileSizeOrHeaders(bSetError, bGetHeaders);
            }

            // If there was no VSI error thrown in the process,
            // fail by reporting the HTTP response code.
            if( bSetError && VSIGetLastErrorNo() == 0 )
            {
                if( strlen(szCurlErrBuf) > 0 )
                {
                    if( response_code == 0 )
                    {
                        VSIError(VSIE_HttpError,
                                 "CURL error: %s", szCurlErrBuf);
                    }
                    else
                    {
                        VSIError(VSIE_HttpError,
                                 "HTTP response code: %d - %s",
                                 static_cast<int>(response_code), szCurlErrBuf);
                    }
                }
                else
                {
                    VSIError(VSIE_HttpError, "HTTP response code: %d",
                             static_cast<int>(response_code));
                }
            }
            else
            {
                if( response_code != 400 && response_code != 404 )
                {
                    CPLError(CE_Warning, CPLE_AppDefined,
                             "HTTP response code on %s: %d",
                             osURL.c_str(), static_cast<int>(response_code));
                }
                // else a CPLDebug() is emitted below
            }

            oFileProp.eExists = EXIST_NO;
            oFileProp.fileSize = 0;
        }
        else if( sWriteFuncData.pBuffer != nullptr )
        {
            ProcessGetFileSizeResult( reinterpret_cast<const char*>(sWriteFuncData.pBuffer) );
        }

        // Try to guess if this is a directory. Generally if this is a
        // directory, curl will retry with an URL with slash added.
        if( !osEffectiveURL.empty() &&
            strncmp(osURL, osEffectiveURL, osURL.size()) == 0 &&
            osEffectiveURL[osURL.size()] == '/' )
        {
            oFileProp.eExists = EXIST_YES;
            oFileProp.fileSize = 0;
            oFileProp.bIsDirectory = true;
        }
        else if( osURL.back() == '/' )
        {
            oFileProp.bIsDirectory = true;
        }

        if( ENABLE_DEBUG && szCurlErrBuf[0] == '\0' )
        {
            CPLDebug(poFS->GetDebugKey(),
                     "GetFileSize(%s)=" CPL_FRMT_GUIB
                     "  response_code=%d",
                     osURL.c_str(), oFileProp.fileSize,
                     static_cast<int>(response_code));
        }
    }

    CPLFree(sWriteFuncData.pBuffer);
    CPLFree(sWriteFuncHeaderData.pBuffer);
    curl_easy_cleanup(hCurlHandle);

    oFileProp.bHasComputedFileSize = true;
    if( mtime > 0 )
        oFileProp.mTime = mtime;
    poFS->SetCachedFileProp(m_pszURL, oFileProp);

    return oFileProp.fileSize;
}

/************************************************************************/
/*                                 Exists()                             */
/************************************************************************/

bool VSICurlHandle::Exists( bool bSetError )
{
    if( oFileProp.eExists == EXIST_UNKNOWN )
    {
        GetFileSize(bSetError);
    }
    return oFileProp.eExists == EXIST_YES;
}

/************************************************************************/
/*                                  Tell()                              */
/************************************************************************/

vsi_l_offset VSICurlHandle::Tell()
{
    return curOffset;
}

/************************************************************************/
/*                       GetRedirectURLIfValid()                        */
/************************************************************************/

CPLString VSICurlHandle::GetRedirectURLIfValid(bool& bHasExpired)
{
    bHasExpired = false;
    poFS->GetCachedFileProp(m_pszURL, oFileProp);

    CPLString osURL(m_pszURL + m_osQueryString);
    if( oFileProp.bS3LikeRedirect )
    {
        if( time(nullptr) + 1 < oFileProp.nExpireTimestampLocal )
        {
            CPLDebug(poFS->GetDebugKey(),
                     "Using redirect URL as it looks to be still valid "
                     "(%d seconds left)",
                     static_cast<int>(oFileProp.nExpireTimestampLocal - time(nullptr)));
            osURL = oFileProp.osRedirectURL;
        }
        else
        {
            CPLDebug(poFS->GetDebugKey(),
                     "Redirect URL has expired. Using original URL");
            oFileProp.bS3LikeRedirect = false;
            poFS->SetCachedFileProp(m_pszURL, oFileProp);
            bHasExpired = true;
        }
    }
    return osURL;
}

/************************************************************************/
/*                          DownloadRegion()                            */
/************************************************************************/

std::string VSICurlHandle::DownloadRegion( const vsi_l_offset startOffset,
                                           const int nBlocks )
{
    if( bInterrupted && bStopOnInterruptUntilUninstall )
        return std::string();

    if( oFileProp.eExists == EXIST_NO )
        return std::string();

    CURLM* hCurlMultiHandle = poFS->GetCurlMultiHandleFor(m_pszURL);

    bool bHasExpired = false;
    CPLString osURL(GetRedirectURLIfValid(bHasExpired));
    bool bUsedRedirect = osURL != m_pszURL;

    WriteFuncStruct sWriteFuncData;
    WriteFuncStruct sWriteFuncHeaderData;
    int nRetryCount = 0;
    double dfRetryDelay = m_dfRetryDelay;

retry:
    CURL* hCurlHandle = curl_easy_init();

    struct curl_slist* headers =
        VSICurlSetOptions(hCurlHandle, osURL, m_papszHTTPOptions);

    if( !AllowAutomaticRedirection() )
        curl_easy_setopt(hCurlHandle, CURLOPT_FOLLOWLOCATION, 0);

    VSICURLInitWriteFuncStruct(&sWriteFuncData,
                               reinterpret_cast<VSILFILE *>(this),
                               pfnReadCbk, pReadCbkUserData);
    curl_easy_setopt(hCurlHandle, CURLOPT_WRITEDATA, &sWriteFuncData);
    curl_easy_setopt(hCurlHandle, CURLOPT_WRITEFUNCTION,
                     VSICurlHandleWriteFunc);

    VSICURLInitWriteFuncStruct(&sWriteFuncHeaderData, nullptr, nullptr, nullptr);
    curl_easy_setopt(hCurlHandle, CURLOPT_HEADERDATA, &sWriteFuncHeaderData);
    curl_easy_setopt(hCurlHandle, CURLOPT_HEADERFUNCTION,
                     VSICurlHandleWriteFunc);
    sWriteFuncHeaderData.bIsHTTP = STARTS_WITH(m_pszURL, "http");
    sWriteFuncHeaderData.nStartOffset = startOffset;
    sWriteFuncHeaderData.nEndOffset =
        startOffset + nBlocks * VSICURLGetDownloadChunkSize() - 1;
    // Some servers don't like we try to read after end-of-file (#5786).
    if( oFileProp.bHasComputedFileSize &&
        sWriteFuncHeaderData.nEndOffset >= oFileProp.fileSize )
    {
        sWriteFuncHeaderData.nEndOffset = oFileProp.fileSize - 1;
    }

    char rangeStr[512] = {};
    snprintf(rangeStr, sizeof(rangeStr),
             CPL_FRMT_GUIB "-" CPL_FRMT_GUIB, startOffset,
            sWriteFuncHeaderData.nEndOffset);

    if( ENABLE_DEBUG )
        CPLDebug(poFS->GetDebugKey(), "Downloading %s (%s)...", rangeStr, osURL.c_str());

    CPLString osHeaderRange; // leave in this scope
    if( sWriteFuncHeaderData.bIsHTTP )
    {
        osHeaderRange.Printf("Range: bytes=%s", rangeStr);
        // So it gets included in Azure signature
        headers = curl_slist_append(headers, osHeaderRange.c_str());
        curl_easy_setopt(hCurlHandle, CURLOPT_RANGE, nullptr);
    }
    else
        curl_easy_setopt(hCurlHandle, CURLOPT_RANGE, rangeStr);

    char szCurlErrBuf[CURL_ERROR_SIZE+1] = {};
    szCurlErrBuf[0] = '\0';
    curl_easy_setopt(hCurlHandle, CURLOPT_ERRORBUFFER, szCurlErrBuf );

    headers = VSICurlMergeHeaders(headers, GetCurlHeaders("GET", headers));
    curl_easy_setopt(hCurlHandle, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(hCurlHandle, CURLOPT_FILETIME, 1);

    MultiPerform(hCurlMultiHandle, hCurlHandle);

    VSICURLResetHeaderAndWriterFunctions(hCurlHandle);

    curl_slist_free_all(headers);

    // NetworkStatisticsLogger::LogGET(sWriteFuncData.nSize);

    if( sWriteFuncData.bInterrupted )
    {
        bInterrupted = true;

        CPLFree(sWriteFuncData.pBuffer);
        CPLFree(sWriteFuncHeaderData.pBuffer);
        curl_easy_cleanup(hCurlHandle);

        return std::string();
    }

    long response_code = 0;
    curl_easy_getinfo(hCurlHandle, CURLINFO_HTTP_CODE, &response_code);

    if( ENABLE_DEBUG && szCurlErrBuf[0] != '\0' )
    {
        CPLDebug(poFS->GetDebugKey(),
                 "DownloadRegion(%s): response_code=%d, msg=%s",
                 osURL.c_str(),
                 static_cast<int>(response_code),
                 szCurlErrBuf);
    }

    long mtime = 0;
    curl_easy_getinfo(hCurlHandle, CURLINFO_FILETIME, &mtime);
    if( mtime > 0 )
    {
        oFileProp.mTime = mtime;
        poFS->SetCachedFileProp(m_pszURL, oFileProp);
    }

    if( ENABLE_DEBUG )
        CPLDebug(poFS->GetDebugKey(),
                 "Got response_code=%ld", response_code);

    if( response_code == 403 && bUsedRedirect )
    {
        CPLDebug(poFS->GetDebugKey(),
                 "Got an error with redirect URL. Retrying with original one");
        oFileProp.bS3LikeRedirect = false;
        poFS->SetCachedFileProp(m_pszURL, oFileProp);
        bUsedRedirect = false;
        osURL = m_pszURL;
        CPLFree(sWriteFuncData.pBuffer);
        CPLFree(sWriteFuncHeaderData.pBuffer);
        curl_easy_cleanup(hCurlHandle);
        goto retry;
    }

    if( response_code == 401 && nRetryCount < m_nMaxRetry )
    {
        CPLDebug(poFS->GetDebugKey(),
                 "Unauthorized, trying to authenticate");
        CPLFree(sWriteFuncData.pBuffer);
        CPLFree(sWriteFuncHeaderData.pBuffer);
        curl_easy_cleanup(hCurlHandle);
        nRetryCount++;
        if( Authenticate() )
            goto retry;
        return std::string();
    }

    CPLString osEffectiveURL;
    {
        char *pszEffectiveURL = nullptr;
        curl_easy_getinfo(hCurlHandle, CURLINFO_EFFECTIVE_URL, &pszEffectiveURL);
        if( pszEffectiveURL )
            osEffectiveURL = pszEffectiveURL;
    }

    if( !oFileProp.bS3LikeRedirect && !osEffectiveURL.empty() &&
        strstr(osEffectiveURL, m_pszURL) == nullptr )
    {
        CPLDebug(poFS->GetDebugKey(),
                 "Effective URL: %s", osEffectiveURL.c_str());
        if( response_code >= 200 && response_code < 300 &&
            sWriteFuncHeaderData.nTimestampDate > 0 &&
            VSICurlIsS3LikeSignedURL(osEffectiveURL) &&
            !VSICurlIsS3LikeSignedURL(m_pszURL) &&
            CPLTestBool(CPLGetConfigOption("CPL_VSIL_CURL_USE_S3_REDIRECT",
                                           "TRUE")) )
        {
            GIntBig nExpireTimestamp =
                VSICurlGetExpiresFromS3LikeSignedURL(osEffectiveURL);
            if( nExpireTimestamp > sWriteFuncHeaderData.nTimestampDate + 10 )
            {
                const int nValidity =
                    static_cast<int>(nExpireTimestamp -
                                     sWriteFuncHeaderData.nTimestampDate);
                CPLDebug(poFS->GetDebugKey(),
                         "Will use redirect URL for the next %d seconds",
                         nValidity);
                // As our local clock might not be in sync with server clock,
                // figure out the expiration timestamp in local time.
                oFileProp.bS3LikeRedirect = true;
                oFileProp.nExpireTimestampLocal = time(nullptr) + nValidity;
                oFileProp.osRedirectURL = osEffectiveURL;
                poFS->SetCachedFileProp(m_pszURL, oFileProp);
            }
        }
    }

    if( (response_code != 200 && response_code != 206 &&
         response_code != 225 && response_code != 226 &&
         response_code != 426) ||
        sWriteFuncHeaderData.bError )
    {
        if( sWriteFuncData.pBuffer != nullptr &&
            CanRestartOnError(reinterpret_cast<const char*>(sWriteFuncData.pBuffer),
                              reinterpret_cast<const char*>(sWriteFuncHeaderData.pBuffer), false) )
        {
            CPLFree(sWriteFuncData.pBuffer);
            CPLFree(sWriteFuncHeaderData.pBuffer);
            curl_easy_cleanup(hCurlHandle);
            return DownloadRegion(startOffset, nBlocks);
        }

        // Look if we should attempt a retry
        const double dfNewRetryDelay = CPLHTTPGetNewRetryDelay(
            static_cast<int>(response_code), dfRetryDelay,
            sWriteFuncHeaderData.pBuffer, szCurlErrBuf);
        if( dfNewRetryDelay > 0 &&
            nRetryCount < m_nMaxRetry )
        {
            CPLError(CE_Warning, CPLE_AppDefined,
                        "HTTP error code: %d - %s. "
                        "Retrying again in %.1f secs",
                        static_cast<int>(response_code), m_pszURL,
                        dfRetryDelay);
            CPLSleep(dfRetryDelay);
            dfRetryDelay = dfNewRetryDelay;
            nRetryCount++;
            CPLFree(sWriteFuncData.pBuffer);
            CPLFree(sWriteFuncHeaderData.pBuffer);
            curl_easy_cleanup(hCurlHandle);
            goto retry;
        }

        if( response_code >= 400 && szCurlErrBuf[0] != '\0' )
        {
            if( strcmp(szCurlErrBuf, "Couldn't use REST") == 0 )
                CPLError(
                    CE_Failure, CPLE_AppDefined,
                    "%d: %s, Range downloading not supported by this server!",
                    static_cast<int>(response_code), szCurlErrBuf);
            else
                CPLError(CE_Failure, CPLE_AppDefined, "%d: %s",
                         static_cast<int>(response_code), szCurlErrBuf);
        }
        if( !oFileProp.bHasComputedFileSize && startOffset == 0 )
        {
            oFileProp.bHasComputedFileSize = true;
            oFileProp.fileSize = 0;
            oFileProp.eExists = EXIST_NO;
            poFS->SetCachedFileProp(m_pszURL, oFileProp);
        }
        CPLFree(sWriteFuncData.pBuffer);
        CPLFree(sWriteFuncHeaderData.pBuffer);
        curl_easy_cleanup(hCurlHandle);
        return std::string();
    }

    if( !oFileProp.bHasComputedFileSize && sWriteFuncHeaderData.pBuffer )
    {
        // Try to retrieve the filesize from the HTTP headers
        // if in the form: "Content-Range: bytes x-y/filesize".
        char* pszContentRange =
            strstr(sWriteFuncHeaderData.pBuffer, "Content-Range: bytes ");
        if( pszContentRange == nullptr )
            pszContentRange = strstr(sWriteFuncHeaderData.pBuffer,
                                     "content-range: bytes ");
        if( pszContentRange )
        {
            char* pszEOL = strchr(pszContentRange, '\n');
            if( pszEOL )
            {
                *pszEOL = 0;
                pszEOL = strchr(pszContentRange, '\r');
                if( pszEOL )
                    *pszEOL = 0;
                char* pszSlash = strchr(pszContentRange, '/');
                if( pszSlash )
                {
                    pszSlash++;
                    oFileProp.fileSize =
                        CPLScanUIntBig(pszSlash,
                                       static_cast<int>(strlen(pszSlash)));
                }
            }
        }
        else if( STARTS_WITH(m_pszURL, "ftp") )
        {
            // Parse 213 answer for FTP protocol.
            char* pszSize = strstr(sWriteFuncHeaderData.pBuffer, "213 ");
            if( pszSize )
            {
                pszSize += 4;
                char* pszEOL = strchr(pszSize, '\n');
                if( pszEOL )
                {
                    *pszEOL = 0;
                    pszEOL = strchr(pszSize, '\r');
                    if( pszEOL )
                        *pszEOL = 0;

                    oFileProp.fileSize =
                        CPLScanUIntBig(pszSize,
                                       static_cast<int>(strlen(pszSize)));
                }
            }
        }

        if( oFileProp.fileSize != 0 )
        {
            oFileProp.eExists = EXIST_YES;

            if( ENABLE_DEBUG )
                CPLDebug(poFS->GetDebugKey(),
                         "GetFileSize(%s)=" CPL_FRMT_GUIB
                         "  response_code=%d",
                         m_pszURL, oFileProp.fileSize, static_cast<int>(response_code));

            oFileProp.bHasComputedFileSize = true;
            poFS->SetCachedFileProp(m_pszURL, oFileProp);
        }
    }

    DownloadRegionPostProcess(startOffset, nBlocks,
                              sWriteFuncData.pBuffer,
                              sWriteFuncData.nSize);

    std::string osRet;
    osRet.assign(sWriteFuncData.pBuffer, sWriteFuncData.nSize);

    CPLFree(sWriteFuncData.pBuffer);
    CPLFree(sWriteFuncHeaderData.pBuffer);
    curl_easy_cleanup(hCurlHandle);

    return osRet;
}

/************************************************************************/
/*                      DownloadRegionPostProcess()                     */
/************************************************************************/

void VSICurlHandle::DownloadRegionPostProcess( const vsi_l_offset startOffset,
                                               const int nBlocks,
                                               const char* pBuffer,
                                               size_t nSize )
{
    const int knDOWNLOAD_CHUNK_SIZE = VSICURLGetDownloadChunkSize();
    lastDownloadedOffset = startOffset + nBlocks * knDOWNLOAD_CHUNK_SIZE;

    if( nSize > static_cast<size_t>(nBlocks) * knDOWNLOAD_CHUNK_SIZE )
    {
        if( ENABLE_DEBUG )
            CPLDebug(
                poFS->GetDebugKey(), "Got more data than expected : %u instead of %u",
                static_cast<unsigned int>(nSize),
                static_cast<unsigned int>(nBlocks * knDOWNLOAD_CHUNK_SIZE));
    }

    vsi_l_offset l_startOffset = startOffset;
    while( nSize > 0 )
    {
#if DEBUG_VERBOSE
        if( ENABLE_DEBUG )
            CPLDebug(
                poFS->GetDebugKey(),
                "Add region %u - %u",
                static_cast<unsigned int>(startOffset),
                static_cast<unsigned int>(
                    std::min(static_cast<size_t>(knDOWNLOAD_CHUNK_SIZE), nSize)));
#endif
        const size_t nChunkSize =
            std::min(static_cast<size_t>(knDOWNLOAD_CHUNK_SIZE), nSize);
        poFS->AddRegion(m_pszURL, l_startOffset, nChunkSize, pBuffer);
        l_startOffset += nChunkSize;
        pBuffer += nChunkSize;
        nSize -= nChunkSize;
    }

}

/************************************************************************/
/*                                Read()                                */
/************************************************************************/

size_t VSICurlHandle::Read( void * const pBufferIn, size_t const nSize,
                            size_t const  nMemb )
{
    // NetworkStatisticsFileSystem oContextFS(poFS->GetFSPrefix());
    // NetworkStatisticsFile oContextFile(m_osFilename);
    // NetworkStatisticsAction oContextAction("Read");

    size_t nBufferRequestSize = nSize * nMemb;
    if( nBufferRequestSize == 0 )
        return 0;

    void* pBuffer = pBufferIn;

#if DEBUG_VERBOSE
    CPLDebug(poFS->GetDebugKey(), "offset=%d, size=%d",
             static_cast<int>(curOffset), static_cast<int>(nBufferRequestSize));
#endif

    vsi_l_offset iterOffset = curOffset;
    const int knMAX_REGIONS = GetMaxRegions();
    const int knDOWNLOAD_CHUNK_SIZE = VSICURLGetDownloadChunkSize();
    while( nBufferRequestSize )
    {
        // Don't try to read after end of file.
        poFS->GetCachedFileProp(m_pszURL, oFileProp);
        if( oFileProp.bHasComputedFileSize &&
            iterOffset >= oFileProp.fileSize )
        {
            if( iterOffset == curOffset )
            {
                CPLDebug(poFS->GetDebugKey(), "Request at offset " CPL_FRMT_GUIB
                         ", after end of file", iterOffset);
            }
            break;
        }

        const vsi_l_offset nOffsetToDownload =
                (iterOffset / knDOWNLOAD_CHUNK_SIZE) * knDOWNLOAD_CHUNK_SIZE;
        std::string osRegion;
        std::shared_ptr<std::string> psRegion = poFS->GetRegion(m_pszURL, nOffsetToDownload);
        if( psRegion != nullptr )
        {
            osRegion = *psRegion;
        }
        else
        {
            if( nOffsetToDownload == lastDownloadedOffset )
            {
                // In case of consecutive reads (of small size), we use a
                // heuristic that we will read the file sequentially, so
                // we double the requested size to decrease the number of
                // client/server roundtrips.
                if( nBlocksToDownload < 100 )
                    nBlocksToDownload *= 2;
            }
            else
            {
                // Random reads. Cancel the above heuristics.
                nBlocksToDownload = 1;
            }

            // Ensure that we will request at least the number of blocks
            // to satisfy the remaining buffer size to read.
            const vsi_l_offset nEndOffsetToDownload =
                ((iterOffset + nBufferRequestSize + knDOWNLOAD_CHUNK_SIZE - 1) / knDOWNLOAD_CHUNK_SIZE) *
                knDOWNLOAD_CHUNK_SIZE;
            const int nMinBlocksToDownload =
                static_cast<int>(
                    (nEndOffsetToDownload - nOffsetToDownload) /
                    knDOWNLOAD_CHUNK_SIZE);
            if( nBlocksToDownload < nMinBlocksToDownload )
                nBlocksToDownload = nMinBlocksToDownload;

            // Avoid reading already cached data.
            // Note: this might get evicted if concurrent reads are done, but
            // this should not cause bugs. Just missed optimization.
            for( int i = 1; i < nBlocksToDownload; i++ )
            {
                if( poFS->GetRegion(
                        m_pszURL,
                        nOffsetToDownload + i * knDOWNLOAD_CHUNK_SIZE) != nullptr )
                {
                    nBlocksToDownload = i;
                    break;
                }
            }

            if( nBlocksToDownload > knMAX_REGIONS )
                nBlocksToDownload = knMAX_REGIONS;

            osRegion = DownloadRegion(nOffsetToDownload, nBlocksToDownload);
            if( osRegion.empty() )
            {
                if( !bInterrupted )
                    bEOF = true;
                return 0;
            }
        }

        const vsi_l_offset nRegionOffset = iterOffset - nOffsetToDownload;
        if (osRegion.size() < nRegionOffset)
        {
            if( iterOffset == curOffset )
            {
                CPLDebug(poFS->GetDebugKey(), "Request at offset " CPL_FRMT_GUIB
                         ", after end of file", iterOffset);
            }
            break;
        }

        const int nToCopy = static_cast<int>(
            std::min(static_cast<vsi_l_offset>(nBufferRequestSize),
                     osRegion.size() - nRegionOffset));
        memcpy(pBuffer,
               osRegion.data() + nRegionOffset,
               nToCopy);
        pBuffer = static_cast<char *>(pBuffer) + nToCopy;
        iterOffset += nToCopy;
        nBufferRequestSize -= nToCopy;
        if( osRegion.size() < static_cast<size_t>(knDOWNLOAD_CHUNK_SIZE) &&
            nBufferRequestSize != 0 )
        {
            break;
        }
    }

    const size_t ret = static_cast<size_t>((iterOffset - curOffset) / nSize);
    if( ret != nMemb )
        bEOF = true;

    curOffset = iterOffset;

    return ret;
}

/************************************************************************/
/*                               Write()                                */
/************************************************************************/

size_t VSICurlHandle::Write( const void * /* pBuffer */,
                             size_t /* nSize */,
                             size_t /* nMemb */ )
{
    return 0;
}

/************************************************************************/
/*                                 Eof()                                */
/************************************************************************/

int       VSICurlHandle::Eof()
{
    return bEOF;
}

/************************************************************************/
/*                                 Flush()                              */
/************************************************************************/

int       VSICurlHandle::Flush()
{
    return 0;
}

/************************************************************************/
/*                                  Close()                             */
/************************************************************************/

int       VSICurlHandle::Close()
{
    return 0;
}

/************************************************************************/
/*                   VSICurlFilesystemHandler()                         */
/************************************************************************/

VSICurlFilesystemHandler::VSICurlFilesystemHandler():
    oCacheFileProp{100 * 1024},
    oCacheDirList{1024, 0}
{
    // int result = curl_global_trace("all");
    // CPLDebug(GetDebugKey(), "curl_global_trace: %d", result);
}

/************************************************************************/
/*                           CachedConnection                           */
/************************************************************************/

namespace {
struct CachedConnection
{
    CURLM          *hCurlMultiHandle = nullptr;
    void            clear();

    ~CachedConnection() { clear(); }
};
} // namespace

static thread_local std::map<VSICurlFilesystemHandler*, CachedConnection> g_tls_connectionCache;
static std::map<VSICurlFilesystemHandler*, CachedConnection>& GetConnectionCache()
{
    return g_tls_connectionCache;
}

/************************************************************************/
/*                              clear()                                 */
/************************************************************************/

void CachedConnection::clear()
{
    if( hCurlMultiHandle )
    {
        curl_multi_cleanup(hCurlMultiHandle);
        hCurlMultiHandle = nullptr;
    }
}

/************************************************************************/
/*                  ~VSICurlFilesystemHandler()                         */
/************************************************************************/

//extern "C" int CPL_DLL GDALIsInGlobalDestructor();

VSICurlFilesystemHandler::~VSICurlFilesystemHandler()
{
    VSICurlFilesystemHandler::ClearCache();
    //if( !GDALIsInGlobalDestructor() )
    //{
        GetConnectionCache().erase(this);
    //}

    if( hMutex != nullptr )
        CPLDestroyMutex( hMutex );
    hMutex = nullptr;
}

/************************************************************************/
/*                      AllowCachedDataFor()                            */
/************************************************************************/

bool VSICurlFilesystemHandler::AllowCachedDataFor(const char* pszFilename)
{
    bool bCachedAllowed = true;
    char** papszTokens = CSLTokenizeString2(
        CPLGetConfigOption("CPL_VSIL_CURL_NON_CACHED", ""), ":", 0 );
    for( int i = 0; papszTokens && papszTokens[i]; i++)
    {
        if( STARTS_WITH(pszFilename, papszTokens[i]) )
        {
            bCachedAllowed = false;
            break;
        }
    }
    CSLDestroy(papszTokens);
    return bCachedAllowed;
}

/************************************************************************/
/*                     GetCurlMultiHandleFor()                          */
/************************************************************************/

CURLM* VSICurlFilesystemHandler::GetCurlMultiHandleFor(const CPLString& /*osURL*/)
{
    auto& conn = GetConnectionCache()[this];
    if( conn.hCurlMultiHandle == nullptr )
    {
        conn.hCurlMultiHandle = curl_multi_init();
    }
    return conn.hCurlMultiHandle;
}

/************************************************************************/
/*                          GetRegionCache()                            */
/************************************************************************/

VSICurlFilesystemHandler::RegionCacheType* VSICurlFilesystemHandler::GetRegionCache()
{
    // should be called under hMutex taken
    if( m_poRegionCacheDoNotUseDirectly == nullptr )
    {
        m_poRegionCacheDoNotUseDirectly.reset(new RegionCacheType(static_cast<size_t>(GetMaxRegions())));
    }
    return m_poRegionCacheDoNotUseDirectly.get();
}

/************************************************************************/
/*                          GetRegion()                                 */
/************************************************************************/

std::shared_ptr<std::string>
VSICurlFilesystemHandler::GetRegion( const char* pszURL,
                                     vsi_l_offset nFileOffsetStart )
{
    CPLMutexHolder oHolder( &hMutex );

    const int knDOWNLOAD_CHUNK_SIZE = VSICURLGetDownloadChunkSize();
    nFileOffsetStart =
        (nFileOffsetStart / knDOWNLOAD_CHUNK_SIZE) * knDOWNLOAD_CHUNK_SIZE;

    std::shared_ptr<std::string> out;
    if( GetRegionCache()->tryGet(
        FilenameOffsetPair(std::string(pszURL), nFileOffsetStart), out) )
    {
        return out;
    }

    return nullptr;
}

/************************************************************************/
/*                          AddRegion()                                 */
/************************************************************************/

void VSICurlFilesystemHandler::AddRegion( const char* pszURL,
                                          vsi_l_offset nFileOffsetStart,
                                          size_t nSize,
                                          const char *pData )
{
    CPLMutexHolder oHolder( &hMutex );

    std::shared_ptr<std::string> value(new std::string());
    value->assign(pData, nSize);
    GetRegionCache()->insert(
        FilenameOffsetPair(std::string(pszURL), nFileOffsetStart),
        value);
}

/************************************************************************/
/*                         GetCachedFileProp()                          */
/************************************************************************/

bool
VSICurlFilesystemHandler::GetCachedFileProp( const char* pszURL,
                                             FileProp& oFileProp )
{
    CPLMutexHolder oHolder( &hMutex );

    return oCacheFileProp.tryGet(std::string(pszURL), oFileProp) &&
            // Let a chance to use new auth parameters
           !(oFileProp.eExists == EXIST_NO &&
             gnGenerationAuthParameters != oFileProp.nGenerationAuthParameters);
}

/************************************************************************/
/*                         SetCachedFileProp()                          */
/************************************************************************/

void
VSICurlFilesystemHandler::SetCachedFileProp( const char* pszURL,
                                             FileProp& oFileProp )
{
    CPLMutexHolder oHolder( &hMutex );

    oFileProp.nGenerationAuthParameters = gnGenerationAuthParameters;
    oCacheFileProp.insert(std::string(pszURL), oFileProp);
}

/************************************************************************/
/*                         GetCachedDirList()                           */
/************************************************************************/

bool
VSICurlFilesystemHandler::GetCachedDirList( const char* pszURL,
                                            CachedDirList& oCachedDirList )
{
    CPLMutexHolder oHolder( &hMutex );

    return oCacheDirList.tryGet(std::string(pszURL), oCachedDirList) &&
            // Let a chance to use new auth parameters
           gnGenerationAuthParameters == oCachedDirList.nGenerationAuthParameters;
}

/************************************************************************/
/*                         SetCachedDirList()                           */
/************************************************************************/

void
VSICurlFilesystemHandler::SetCachedDirList( const char* pszURL,
                                            CachedDirList& oCachedDirList )
{
    CPLMutexHolder oHolder( &hMutex );

    std::string key(pszURL);
    CachedDirList oldValue;
    if( oCacheDirList.tryGet(key, oldValue) )
    {
        nCachedFilesInDirList -= oldValue.oFileList.size();
        oCacheDirList.remove(key);
    }

    while( (!oCacheDirList.empty() &&
            nCachedFilesInDirList + oCachedDirList.oFileList.size() > 1024 * 1024) ||
            oCacheDirList.size() == oCacheDirList.getMaxAllowedSize() )
    {
        std::string oldestKey;
        oCacheDirList.getOldestEntry(oldestKey, oldValue);
        nCachedFilesInDirList -= oldValue.oFileList.size();
        oCacheDirList.remove(oldestKey);
    }
    oCachedDirList.nGenerationAuthParameters = gnGenerationAuthParameters;

    nCachedFilesInDirList += oCachedDirList.oFileList.size();
    oCacheDirList.insert(key, oCachedDirList);
}

/************************************************************************/
/*                        ExistsInCacheDirList()                        */
/************************************************************************/

bool VSICurlFilesystemHandler::ExistsInCacheDirList(
                            const CPLString& osDirname, bool *pbIsDir )
{
    CachedDirList cachedDirList;
    if( GetCachedDirList(osDirname, cachedDirList) )
    {
        if( pbIsDir )
            *pbIsDir = !cachedDirList.oFileList.empty();
        return false;
    }
    else
    {
        if( pbIsDir )
            *pbIsDir = false;
        return false;
    }
}

/************************************************************************/
/*                        InvalidateCachedData()                        */
/************************************************************************/

void VSICurlFilesystemHandler::InvalidateCachedData( const char* pszURL )
{
    CPLMutexHolder oHolder( &hMutex );

    oCacheFileProp.remove(std::string(pszURL));

    // Invalidate all cached regions for this URL
    std::list<FilenameOffsetPair> keysToRemove;
    std::string osURL(pszURL);
    auto lambda = [&keysToRemove, &osURL](
        const lru11::KeyValuePair<FilenameOffsetPair,
                                  std::shared_ptr<std::string>>& kv)
    {
        if( kv.key.filename_ == osURL )
            keysToRemove.push_back(kv.key);
    };
    auto* poRegionCache = GetRegionCache();
    poRegionCache->cwalk(lambda);
    for( auto& key: keysToRemove )
        poRegionCache->remove(key);
}

/************************************************************************/
/*                            ClearCache()                              */
/************************************************************************/

void VSICurlFilesystemHandler::ClearCache()
{
    CPLMutexHolder oHolder( &hMutex );

    GetRegionCache()->clear();

    oCacheFileProp.clear();

    oCacheDirList.clear();
    nCachedFilesInDirList = 0;

    //if( !GDALIsInGlobalDestructor() )
    //{
        GetConnectionCache()[this].clear();
    //}
}

/************************************************************************/
/*                          PartialClearCache()                         */
/************************************************************************/

void VSICurlFilesystemHandler::PartialClearCache(const char* pszFilenamePrefix)
{
    CPLMutexHolder oHolder( &hMutex );

    CPLString osURL = GetURLFromFilename(pszFilenamePrefix);
    {
        std::list<FilenameOffsetPair> keysToRemove;
        auto lambda = [&keysToRemove, &osURL](
            const lru11::KeyValuePair<FilenameOffsetPair,
                                                std::shared_ptr<std::string>>& kv)
        {
            if( strncmp(kv.key.filename_.c_str(), osURL, osURL.size()) == 0 )
                keysToRemove.push_back(kv.key);
        };
        auto* poRegionCache = GetRegionCache();
        poRegionCache->cwalk(lambda);
        for( auto& key: keysToRemove )
            poRegionCache->remove(key);
    }

    {
        std::list<std::string> keysToRemove;
        auto lambda = [&keysToRemove, &osURL](
            const lru11::KeyValuePair<std::string, FileProp>& kv)
        {
            if( strncmp(kv.key.c_str(), osURL, osURL.size()) == 0 )
                keysToRemove.push_back(kv.key);
        };
        oCacheFileProp.cwalk(lambda);
        for( auto& key: keysToRemove )
            oCacheFileProp.remove(key);
    }

    {
        const size_t nLen = strlen(pszFilenamePrefix);
        std::list<std::string> keysToRemove;
        auto lambda = [this, &keysToRemove, pszFilenamePrefix, nLen](
            const lru11::KeyValuePair<std::string, CachedDirList>& kv)
        {
            if( strncmp(kv.key.c_str(), pszFilenamePrefix, nLen) == 0 )
            {
                keysToRemove.push_back(kv.key);
                nCachedFilesInDirList -= kv.value.oFileList.size();
            }
        };
        oCacheDirList.cwalk(lambda);
        for( auto& key: keysToRemove )
            oCacheDirList.remove(key);
    }
}

/************************************************************************/
/*                          CreateFileHandle()                          */
/************************************************************************/

VSICurlHandle* VSICurlFilesystemHandler::CreateFileHandle(
                                                const char* pszFilename )
{
    return new VSICurlHandle(this, pszFilename);
}

/************************************************************************/
/*                          GetActualURL()                              */
/************************************************************************/

const char* VSICurlFilesystemHandler::GetActualURL(const char* pszFilename)
{
    VSICurlHandle* poHandle = CreateFileHandle(pszFilename);
    if( poHandle == nullptr )
        return pszFilename;
    CPLString osURL(poHandle->GetURL());
    delete poHandle;
    return CPLSPrintf("%s", osURL.c_str());
}

/************************************************************************/
/*                                Open()                                */
/************************************************************************/

VSIVirtualHandle* VSICurlFilesystemHandler::Open( const char *pszFilename,
                                                  const char *pszAccess,
                                                  bool /* bSetError */,
                                                  CSLConstList /* papszOptions */ )
{
    if( !STARTS_WITH_CI(pszFilename, GetFSPrefix()) &&
        !STARTS_WITH_CI(pszFilename, "/vsicurl?") )
        return nullptr;

    if( strchr(pszAccess, 'w') != nullptr ||
        strchr(pszAccess, '+') != nullptr )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Only read-only mode is supported for /vsicurl");
        return nullptr;
    }
    CPLString osFilename(pszFilename);
    VSICurlHandle* poHandle =
        CreateFileHandle(osFilename);
    if( poHandle == nullptr )
        return nullptr;

    return poHandle;
}

/************************************************************************/
/*                          GetURLFromFilename()                         */
/************************************************************************/

CPLString
VSICurlFilesystemHandler::GetURLFromFilename( const CPLString& osFilename )
{
    return VSICurlGetURLFromFilename(osFilename, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);
}

/************************************************************************/
/*                     VSICurlParseUnixPermissions()                    */
/************************************************************************/

int VSICurlParseUnixPermissions(const char* pszPermissions)
{
    if( strlen(pszPermissions) != 9 )
        return 0;
    int nMode = 0;
    if( pszPermissions[0] == 'r' )
        nMode |= S_IRUSR;
    if( pszPermissions[1] == 'w' )
        nMode |= S_IWUSR;
    if( pszPermissions[2] == 'x' )
        nMode |= S_IXUSR;
    if( pszPermissions[3] == 'r' )
        nMode |= S_IRGRP;
    if( pszPermissions[4] == 'w' )
        nMode |= S_IWGRP;
    if( pszPermissions[5] == 'x' )
        nMode |= S_IXGRP;
    if( pszPermissions[6] == 'r' )
        nMode |= S_IROTH;
    if( pszPermissions[7] == 'w' )
        nMode |= S_IWOTH;
    if( pszPermissions[8] == 'x' )
        nMode |= S_IXOTH;
    return nMode;
}


} /* end of namespace cpl */

/************************************************************************/
/*                      VSICurlInstallReadCbk()                         */
/************************************************************************/

int VSICurlInstallReadCbk( VSILFILE* fp,
                           VSICurlReadCbkFunc pfnReadCbk,
                           void* pfnUserData,
                           int bStopOnInterruptUntilUninstall )
{
    return reinterpret_cast<cpl::VSICurlHandle *>(fp)->
        InstallReadCbk(pfnReadCbk, pfnUserData, bStopOnInterruptUntilUninstall);
}

/************************************************************************/
/*                    VSICurlUninstallReadCbk()                         */
/************************************************************************/

int VSICurlUninstallReadCbk( VSILFILE* fp )
{
    return reinterpret_cast<cpl::VSICurlHandle *>(fp)->UninstallReadCbk();
}

/************************************************************************/
/*                       VSICurlSetOptions()                            */
/************************************************************************/

struct curl_slist* VSICurlSetOptions(
                        CURL* hCurlHandle, const char* pszURL,
                        const char * const* papszOptions )
{
    struct curl_slist* headers = static_cast<struct curl_slist*>(
        CPLHTTPSetOptions(hCurlHandle, pszURL, papszOptions));

    long option = CURLFTPMETHOD_SINGLECWD;
    curl_easy_setopt(hCurlHandle, CURLOPT_FTP_FILEMETHOD, option);

    // ftp://ftp2.cits.rncan.gc.ca/pub/cantopo/250k_tif/
    // doesn't like EPSV command,
    curl_easy_setopt(hCurlHandle, CURLOPT_FTP_USE_EPSV, 0);

    return headers;
}

/************************************************************************/
/*                     VSICurlMergeHeaders()                            */
/************************************************************************/

struct curl_slist* VSICurlMergeHeaders( struct curl_slist* poDest,
                                        struct curl_slist* poSrcToDestroy )
{
    struct curl_slist* iter = poSrcToDestroy;
    while( iter != nullptr )
    {
        poDest = curl_slist_append(poDest, iter->data);
        iter = iter->next;
    }
    if( poSrcToDestroy )
        curl_slist_free_all(poSrcToDestroy);
    return poDest;
}

/************************************************************************/
/*                    VSICurlSetContentTypeFromExt()                    */
/************************************************************************/

struct curl_slist* VSICurlSetContentTypeFromExt(struct curl_slist* poList,
                                                const char *pszPath)
{
    struct curl_slist* iter = poList;
    while( iter != nullptr )
    {
        if( STARTS_WITH_CI(iter->data, "Content-Type") )
        {
            return poList;
        }
        iter = iter->next;
    }

    static const struct
    {
        const char* ext;
        const char* mime;
    }
    aosExtMimePairs[] =
    {
        {"txt", "text/plain"},
        {"json", "application/json"},
        {"tif", "image/tiff"}, {"tiff", "image/tiff"},
        {"jpg", "image/jpeg"}, {"jpeg", "image/jpeg"},
        {"jp2", "image/jp2"}, {"jpx", "image/jp2"}, {"j2k", "image/jp2"}, {"jpc", "image/jp2"},
        {"png", "image/png"},
    };

    const char *pszExt = CPLGetExtension(pszPath);
    if( pszExt )
    {
        for( const auto& pair: aosExtMimePairs )
        {
            if( EQUAL(pszExt, pair.ext) )
            {

                CPLString osContentType;
                osContentType.Printf("Content-Type: %s", pair.mime);
                poList = curl_slist_append(poList, osContentType.c_str());
#ifdef DEBUG_VERBOSE
                CPLDebug("HTTP", "Setting %s, based on lookup table.", osContentType.c_str());
#endif
                break;
            }
        }
    }

    return poList;
}

/************************************************************************/
/*                VSICurlSetCreationHeadersFromOptions()                */
/************************************************************************/

struct curl_slist* VSICurlSetCreationHeadersFromOptions(struct curl_slist* headers,
                                                        CSLConstList papszOptions,
                                                        const char *pszPath)
{
    bool bContentTypeFound = false;
    for( CSLConstList papszIter = papszOptions; papszIter && *papszIter; ++papszIter )
    {
        char* pszKey = nullptr;
        const char* pszValue = CPLParseNameValue(*papszIter, &pszKey);
        if( pszKey && pszValue )
        {
            if( EQUAL(pszKey, "Content-Type") )
            {
                bContentTypeFound = true;
            }
            CPLString osVal;
            osVal.Printf("%s: %s", pszKey, pszValue);
            headers = curl_slist_append(headers, osVal.c_str());
        }
        CPLFree(pszKey);
    }

    // If Content-type not found in papszOptions, try to set it from the
    // filename exstension.
    if( !bContentTypeFound )
    {
        headers = VSICurlSetContentTypeFromExt(headers, pszPath);
    }

    return headers;
}

#endif // DOXYGEN_SKIP
//! @endcond

/************************************************************************/
/*                   VSIInstallCurlFileHandler()                        */
/************************************************************************/

/**
 * \brief Install /vsicurl/ HTTP/FTP file system handler (requires libcurl)
 *
 * @see <a href="gdal_virtual_file_systems.html#gdal_virtual_file_systems_vsicurl">/vsicurl/ documentation</a>
 *
 * @since GDAL 1.8.0
 */
void VSIInstallCurlFileHandler( void )
{
    VSIFilesystemHandler* poHandler = new cpl::VSICurlFilesystemHandler;
    VSIFileManager::InstallHandler( "/vsicurl/", poHandler );
    VSIFileManager::InstallHandler( "/vsicurl?", poHandler );
}

/************************************************************************/
/*                         VSICurlClearCache()                          */
/************************************************************************/

/**
 * \brief Clean local cache associated with /vsicurl/ (and related file systems)
 *
 * /vsicurl (and related file systems like /vsis3/, /vsigs/, /vsiaz/, /vsioss/,
 * /vsiswift/) cache a number of
 * metadata and data for faster execution in read-only scenarios. But when the
 * content on the server-side may change during the same process, those
 * mechanisms can prevent opening new files, or give an outdated version of them.
 *
 * @since GDAL 2.2.1
 */

void VSICurlClearCache( void )
{
    // FIXME ? Currently we have different filesystem instances for
    // vsicurl/, /vsis3/, /vsigs/ . So each one has its own cache of regions,
    // file size, etc.
    CSLConstList papszPrefix = VSIFileManager::GetPrefixes();
    for( size_t i = 0; papszPrefix && papszPrefix[i]; ++i )
    {
        auto poFSHandler =
            dynamic_cast<cpl::VSICurlFilesystemHandler*>(
                VSIFileManager::GetHandler( papszPrefix[i] ));

        if( poFSHandler )
            poFSHandler->ClearCache();
    }

    //VSICurlStreamingClearCache();
}

/************************************************************************/
/*                      VSICurlPartialClearCache()                      */
/************************************************************************/

/**
 * \brief Clean local cache associated with /vsicurl/ (and related file systems)
 * for a given filename (and its subfiles and subdirectories if it is a
 * directory)
 *
 * /vsicurl (and related file systems like /vsis3/, /vsigs/, /vsiaz/, /vsioss/,
 * /vsiswift/) cache a number of
 * metadata and data for faster execution in read-only scenarios. But when the
 * content on the server-side may change during the same process, those
 * mechanisms can prevent opening new files, or give an outdated version of them.
 *
 * @param pszFilenamePrefix Filename prefix
 * @since GDAL 2.4.0
 */

void VSICurlPartialClearCache(const char* pszFilenamePrefix)
{
     auto poFSHandler =
            dynamic_cast<cpl::VSICurlFilesystemHandler*>(
                VSIFileManager::GetHandler( pszFilenamePrefix ));

    if( poFSHandler )
        poFSHandler->PartialClearCache(pszFilenamePrefix);
}

#endif /* HAVE_CURL */
