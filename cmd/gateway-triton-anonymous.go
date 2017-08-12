/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// Make anonymous HTTP request to azure endpoint.
func tritonAnonRequest(verb, urlStr string, header http.Header) (*http.Response, error) {
	req, err := http.NewRequest(verb, urlStr, nil)
	if err != nil {
		return nil, err
	}
	if header != nil {
		req.Header = header
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	// 4XX and 5XX are error HTTP codes.
	if resp.StatusCode >= 400 && resp.StatusCode <= 511 {
		defer resp.Body.Close()
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		if len(respBody) == 0 {
			// no error in response body, might happen in HEAD requests
			return nil, storage.AzureStorageServiceError{
				StatusCode: resp.StatusCode,
				Code:       resp.Status,
				Message:    "no response body was available for error status code",
			}
		}
		// Response contains Azure storage service error object.
		var storageErr storage.AzureStorageServiceError
		if err := xml.Unmarshal(respBody, &storageErr); err != nil {
			return nil, err
		}
		storageErr.StatusCode = resp.StatusCode
		return nil, storageErr
	}

	return resp, nil
}

// AnonGetBucketInfo - Get bucket metadata from azure anonymously.
func (a *tritonObjects) AnonGetBucketInfo(bucket string) (bucketInfo BucketInfo, err error) {
	return bucketInfo, nil
}

// AnonPutObject - SendPUT request without authentication.
// This is needed when clients send PUT requests on objects that can be uploaded without auth.
func (a *tritonObjects) AnonPutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	// azure doesn't support anonymous put
	return ObjectInfo{}, traceError(NotImplemented{})
}

// AnonGetObject - SendGET request without authentication.
// This is needed when clients send GET requests on objects that can be downloaded without auth.
func (a *tritonObjects) AnonGetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	return nil
}

// AnonGetObjectInfo - Send HEAD request without authentication and convert the
// result to ObjectInfo.
func (a *tritonObjects) AnonGetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	return objInfo, nil
}

// AnonListObjects - Use Azure equivalent ListBlobs.
func (a *tritonObjects) AnonListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	return result, nil
}

// AnonListObjectsV2 - List objects in V2 mode, anonymously
func (a *tritonObjects) AnonListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (result ListObjectsV2Info, err error) {
	return result, nil
}
