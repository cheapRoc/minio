package cmd

import (
	"context"
	"errors"
	"io"
	"os"

	triton "github.com/joyent/triton-go"
	"github.com/joyent/triton-go/authentication"
	"github.com/joyent/triton-go/storage"
	"github.com/minio/minio-go/pkg/policy"
)

const DefaultMantaURL = "https://us-east.manta.joyent.com"

// tritonObjects - Implements Object layer for Triton Manta storage
type tritonObjects struct {
	client *storage.StorageClient
}

func newTritonGateway(host string) (GatewayLayer, error) {
	var endpoint = DefaultMantaURL
	// var secure = true

	// if host != "" {
	// 	endpoint, _, err = parseGatewayEndpoint(host)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	keyID, foundKey := os.LookupEnv("MANTA_KEY_ID")
	if !foundKey {
		return nil, errors.New("Couldn't find \"MANTA_KEY_ID\" in your environment")
	}

	accountName, foundAccount := os.LookupEnv("MANTA_USER")
	if !foundAccount {
		return nil, errors.New("Couldn't find \"MANTA_USER\" in your environment")
	}

	mantaURL, foundURL := os.LookupEnv("MANTA_URL")
	if foundURL {
		endpoint = mantaURL
	}

	signer, err := authentication.NewSSHAgentSigner(keyID, accountName)
	if err != nil {
		return nil, err
	}

	config := &triton.ClientConfig{
		MantaURL:    endpoint,
		AccountName: accountName,
		Signers:     []authentication.Signer{signer},
	}
	triton, err := storage.NewClient(config)
	if err != nil {
		return nil, err
	}

	return &tritonObjects{
		client: triton,
	}, nil
}

// Shutdown - save any gateway metadata to disk
// if necessary and reload upon next restart.
func (t *tritonObjects) Shutdown() error {
	return nil
}

// StorageInfo - Not relevant to Triton backend.
func (t *tritonObjects) StorageInfo() (si StorageInfo) {
	return si
}

//
// ~~~ Buckets ~~~
//

// MakeBucketWithLocation - Create a new directory within manta.
//
// https://apidocs.joyent.com/manta/api.html#PutDirectory
func (t *tritonObjects) MakeBucketWithLocation(bucket, location string) error {
	ctx := context.Background()
	err := t.client.Dir().Put(ctx, &storage.PutDirectoryInput{
		DirectoryName: "~~/stor/" + bucket,
	})
	if err != nil {
		return err
	}
	return nil
	// return tritonToObjectError(traceError(err), bucket)
}

// GetBucketInfo - Get directory metadata..
//
// https://apidocs.joyent.com/manta/api.html#GetObject
func (a *tritonObjects) GetBucketInfo(bucket string) (bi BucketInfo, e error) {
	return bi, nil
}

// ListBuckets - Lists all Manta directories, uses Manta equivalent
// ListDirectories.
//
// https://apidocs.joyent.com/manta/api.html#ListDirectory
func (a *tritonObjects) ListBuckets() (buckets []BucketInfo, err error) {
	return buckets, nil
}

// DeleteBucket - Delete a directory in Manta, uses Manta equivalent
// DeleteDirectory.
//
// https://apidocs.joyent.com/manta/api.html#DeleteDirectory
func (a *tritonObjects) DeleteBucket(bucket string) error {
	return nil
}

//
// ~~~ Objects ~~~
//

// ListObjects - Lists all objects in Manta with a container filtered by prefix
// and marker, uses Manta equivalent ListDirectory.
//
// https://apidocs.joyent.com/manta/api.html#ListDirectory
func (a *tritonObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	return result, nil
}

// ListObjectsV2 - List all objects in a Manta directory filtered by prefix.
//
// https://apidocs.joyent.com/manta/api.html#ListDirectory
func (a *tritonObjects) ListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (result ListObjectsV2Info, err error) {
	return result, nil
}

// GetObject - Reads an object from Manta. Supports additional parameters like
// offset and length which are synonymous with HTTP Range requests.
//
// startOffset indicates the starting read location of the object.  length
// indicates the total length of the object.
//
// https://apidocs.joyent.com/manta/api.html#GetObject
func (a *tritonObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) error {
	return nil
}

// GetObjectInfo - reads blob metadata properties and replies back ObjectInfo,
// uses Triton equivalent GetBlobProperties.
//
// https://apidocs.joyent.com/manta/api.html#GetObject
func (a *tritonObjects) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	return objInfo, nil
}

// PutObject - Create a new blob with the incoming data, uses Triton equivalent
// CreateBlockBlobFromReader.
//
// https://apidocs.joyent.com/manta/api.html#PutObject
func (a *tritonObjects) PutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	return objInfo, nil
}

// CopyObject - Copies a blob from source container to destination container.
// Uses Azure equivalent CopyBlob API.
func (a *tritonObjects) CopyObject(srcBucket, srcObject, destBucket, destObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {
	return objInfo, nil
}

// DeleteObject - Delete a blob in Manta, uses Triton equivalent DeleteBlob API.
//
// https://apidocs.joyent.com/manta/api.html#DeleteObject
func (a *tritonObjects) DeleteObject(bucket, object string) error {
	return nil
}

//
// ~~~ MPU ~~~
//

func (a *tritonObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	return result, nil
}

func (a *tritonObjects) NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	return uploadID, nil
}

func (a *tritonObjects) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (info PartInfo, err error) {
	return info, nil
}

func (a *tritonObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
	return result, nil
}

func (t *tritonObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	return nil
}

func (a *tritonObjects) CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []completePart) (objInfo ObjectInfo, err error) {
	return objInfo, nil
}

// CopyObjectPart - Not implemented.
func (a *tritonObjects) CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	return info, traceError(NotImplemented{})
}

//
// ~~~ Bucket Policy ~~~
//
func (a *tritonObjects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	return nil
}

func (a *tritonObjects) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	return policy.BucketAccessPolicy{}, nil
}

// DeleteBucketPolicies - Set the container ACL to "private"
func (a *tritonObjects) DeleteBucketPolicies(bucket string) error {
	return nil
}

//
//
//
