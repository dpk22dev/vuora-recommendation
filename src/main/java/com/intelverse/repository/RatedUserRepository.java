package com.intelverse.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.intelverse.dto.RatedUser;

@Repository
public interface RatedUserRepository extends MongoRepository<RatedUser, String> {

}
