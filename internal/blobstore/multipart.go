package blobstore

// uploadDoc describes the record that's held
// for a pending multipart upload.
type uploadDoc struct {
	// Id holds the upload id. The blob for each
	// part in the underlying blobstore will be named
	// $id/$partnumber.
	Id string			`bson:"_id"`

	// Hash holds the SHA384 hash of all the
	// concatenated parts. It is empty until
	// after FinishParts is called.
	Hash string		`bson:"hash,omitempty"`

	// Expires holds the expiry time of the upload.
	Expires time.Time

	// Parts holds all the currently uploaded parts.
	Parts []*uploadPart
}

// {$or {$eq {.parts.7: {expectedPart}} {$eq {.parts.7 null}}}}

// $set: {parts.7: 

// uploadPart
type uploadPart struct {
	// Hash holds the SHA384 hash of the part.
	Hash string
	// Size holds the size of the part.
	Size int64
	// Complete holds whether the part has been
	// successfully uploaded.
	Complete bool
}

// NewParts created a new multipart entry to track
// a multipart upload. It returns an uploadId that
// can be used to refer to it.
func (s *Store) NewParts(expiry time.Time) (uploadId string, err error) {
	uploadId := fmt.Sprintf("%x", bson.NewObjectId())
	if err := s.uploadC().Insert(uploadDoc{
		Id: uploadId,
		Expires: expiry,
	}); err != nil {
		return "", errgo.Notef(err, "cannot create new upload")
	}
	return uploadId, nil
}

// ListParts returns all the parts associated with the given
// upload id. It returns ErrNotFound if the upload has been
// deleted or finished.
func (s *Store) ListParts(uploadId string) ([]Part, error) {
	return nil, errgo.New("parts listing not implemented yet")
	// read multipart metadata
	// return parts from that, omitting parts that are currently in progress
}

// PutPart uploads a part to the given upload id. The part number
// is specified with the part parameter; its content will be read from r
// and is expected to have the given size and hex-encoded SHA384 hash.
// A given part may not be uploaded more than once with a different hash.
//
// If the upload id was not found (for example, because it's expired),
// PutPart returns an error with an ErrNotFound cause.
func (s *Store) PutPart(uploadId string, part int, r io.Reader, size int64, hash string) error {
	if part < 0 {
		return errgo.Newf("negative part number")
	}
	if part > maxParts {
		return errgo.Newf("part number too big")
	}
	uploadc := s.uploadC()
	var udoc uploadDoc
	if err := uploadc.FindId(uploadId).One(&udoc); err != nil {
		if err == mgo.ErrNotFound {
			return errgo.WithCausef(nil, ErrNotFound, "upload id %q not found", uploadId)
		}
		return errgo.Notef(err, "cannot get upload id %q", uploadId)
	}
	partElem := fmt.Sprintf("parts.%d", part)
	if part < len(udoc.Parts) && udoc.Parts[part] != nil {
		// There's already a (possibly complete) part record stored.
		p := udoc.Parts[part]
		if p.Hash != hash {
			return errgo.Newf("hash mismatch for already uploaded part")
		}
		if p.Complete {
			// It's already uploaded, then we can use the existing uploaded part.
			return nil
		}
		// Someone else made the part record, but it's not complete
		// perhaps because a previous part upload failed.
	} else {
		// No part record. Make one, not marked as complete
		// before we put the part so that DeleteExpiredParts
		// knows to delete the part.
		complete, err := initializePart(uploadC, uploadId, hash, size)
		if err != nil {
			return errgo.Mask(err, errgo.Is(ErrNotFound))
		}
		if complete {
			return nil
		}
	}
	// The part record has been updated successfully, so
	// we can actually upload the part now.
	partName := s.partName(uploadId, part)
	if err := s.Put(r, partName, size, hash); err != nil {
		return errgo.Notef(err, "cannot upload part %q", partName)
	}

	// We've put the part document, so we can
	// now mark the part as complete.
	err := uploadc.UpdateId(uploadId, bson.D{{
		"$set", bson.D{{
			partElem + ".complete", true,
		}},
	}})
	if err != nil {
		return errgo.Notef(err, "cannot mark part as complete")
	}
	return nil
}

// initializePart creates the initial record for a part. It returns an
// error with an ErrNotFound cause if the upload id doesn't exist, and
// reports whether the part upload has already completed successfully.
func initializePart(uploadc *gc.C, uploadId string, hash string, size int64) (complete bool, err error) {
	err := uploadc.Update(bson.D{
		{"_id", uploadId},
		{"$or", []bson.D{{{
			partElem, bson.D{{"$exists", false}},
		}}, {{
			partElem, bson.D{{"$eq", nil}},
		}}}},
	},
		bson.D{{
			"$set", bson.D{{partElem, uploadPart{
				Hash: hash,
				Size: size,
			}}},
		}},
	)
	if err == nil {
		return false, nil
	}
	if err != mgo.ErrNotFound {
		return errgo.Notef(err, "cannot update initial part record")
	}
	// The update criteria didn't match any documents, which
	// means that either the update was deleted or another
	// concurrent upload is happening.
	// Fetch the upload document again to find out.
	if err := uploadc.FindId(uploadId).One(&udoc); err != nil {
		if err == mgo.ErrNotFound {
			// It probably expired.
			return errgo.WithCausef(nil, ErrNotFound, "upload id %q not found", uploadId)
		}
		return errgo.Notef(err, "cannot refetch upload id %q", uploadId)
	}
	if part >= len(udoc.Parts) || udoc.Parts[part] == nil {
		// Should never happen, because our update criteria should
		// match in this case.
		return errgo.Newf("part update failed even though part has not been uploaded")
	}
	p := udoc.Parts[part]
	if p.Hash != hash {
		return errgo.Newf("hash mismatch for already uploaded part")
	}
	// The hash matches. Some other client got there first.
	if p.Complete {
		// Their upload has already completed, so use that version.
		return true, nil
	}
	// Go ahead and race for it.
	return false, nil
}

// FinishParts completes a multipart upload by joining all the given
// parts into one blob. The resulting blob can be opened by passing
// uploadId and the returned multipart index to Open.
//
// The part numbers used will be from 0 to len(parts)-1.
//
// This does not delete the multipart metadata, which should still be
// deleted explicitly by calling DeleteParts after the index data is
// stored.
func (s *Store) FinishParts(uploadId string, parts []Part) (idx *MultipartIndex, hash string, err error) {
	
	read metadata
	if parts don't match uploaded parts, return error
	read all parts in sequence to hash them
	return index derived from metadata and calculated hash
}

// DeleteExpiredParts deletes any multipart entries
// that have passed their expiry date.
func (s *Store) DeleteExpiredParts() error

// DeleteParts deletes all the parts associated with the
// given upload id. It does nothing if FinishParts
// has already been called for the given upload id.
func (s *Store) DeleteParts(uploadId string) error {
	read multipart metadata
	delete all parts referenced in that
	delete multipart metadata
}

// MultipartIndex holds the index of all the parts of a multipart blob.
// It should be stored in an external document along with the
// blob name so that the blob can be downloaded.
type MultipartIndex struct {
	Sizes []uint32
}

// Part represents one part of a multipart blob.
type Part struct {
	Hash string
}

func (s *Store) uploadC() *gc.C {
	return s.db.C(prefix + ".upload")
}
