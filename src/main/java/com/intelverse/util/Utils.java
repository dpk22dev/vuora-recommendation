package com.intelverse.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.TimeZone;

public class Utils {

	public static Long LocalDateToLong(LocalDateTime date) {
		return date.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	}

	public static LocalDateTime LongToLocalDateTime(Long date) {
		return LocalDateTime.ofInstant(Instant.ofEpochMilli(date), TimeZone.getDefault().toZoneId());
	}

	public static Long LocalDateTimeToLong(LocalDateTime date) {
		return date.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	}

	public static LocalDate LongToLocalDate(Long date) {
		return Instant.ofEpochMilli(date).atZone(ZoneId.systemDefault()).toLocalDate();
	}

	public static Long LocalDateToLong(LocalDate date) {
		return Instant.from(LocalDateTime.of(date, LocalTime.of(0, 0)).atZone(ZoneId.systemDefault())).toEpochMilli();
	}
}
