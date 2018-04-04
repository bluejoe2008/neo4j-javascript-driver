/**
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
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

import {int} from '../integer';
import {Date, LocalDateTime, LocalTime} from '../temporal-types';

/*
  Code in this util should be compatible with code in the database that uses JSR-310 java.time APIs.

  It is based on a library called ThreeTen (https://github.com/ThreeTen/threetenbp) which was derived
  from JSR-310 reference implementation previously hosted on GitHub. Code uses `Integer` type everywhere
  to correctly handle large integer values that are greater than <code>Number.MAX_SAFE_INTEGER</code>.

  Please consult either ThreeTen or js-joda (https://github.com/js-joda/js-joda) when working with the
  conversion functions.
 */

const MINUTES_PER_HOUR = 60;
const SECONDS_PER_MINUTE = 60;
const SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR;
const NANOS_PER_SECOND = 1000000000;
const NANOS_PER_MINUTE = NANOS_PER_SECOND * SECONDS_PER_MINUTE;
const NANOS_PER_HOUR = NANOS_PER_MINUTE * MINUTES_PER_HOUR;
const DAYS_0000_TO_1970 = 719528;
const DAYS_PER_400_YEAR_CYCLE = 146097;
const SECONDS_PER_DAY = 86400;

/**
 * Converts given local time into a single integer representing this same time in nanoseconds of the day.
 * @param {LocalTime} localTime the time to convert.
 * @return {Integer} nanoseconds representing the given local time.
 */
export function localTimeToNanoOfDay(localTime) {
  const hour = int(localTime.hour);
  const minute = int(localTime.minute);
  const second = int(localTime.second);
  const nanosecond = int(localTime.nanosecond);

  let totalNanos = hour.multiply(NANOS_PER_HOUR);
  totalNanos = totalNanos.add(minute.multiply(NANOS_PER_MINUTE));
  totalNanos = totalNanos.add(second.multiply(NANOS_PER_SECOND));
  return totalNanos.add(nanosecond);
}

/**
 * Converts nanoseconds of the day into local time.
 * @param {Integer|number|string} nanoOfDay the nanoseconds of the day to convert.
 * @return {LocalTime} the local time representing given nanoseconds of the day.
 */
export function nanoOfDayToLocalTime(nanoOfDay) {
  nanoOfDay = int(nanoOfDay);

  const hour = nanoOfDay.div(NANOS_PER_HOUR);
  nanoOfDay = nanoOfDay.subtract(hour.multiply(NANOS_PER_HOUR));

  const minute = nanoOfDay.div(NANOS_PER_MINUTE);
  nanoOfDay = nanoOfDay.subtract(minute.multiply(NANOS_PER_MINUTE));

  const second = nanoOfDay.div(NANOS_PER_SECOND);
  const nanosecond = nanoOfDay.subtract(second.multiply(NANOS_PER_SECOND));

  return new LocalTime(hour, minute, second, nanosecond);
}

/**
 * Converts given local date time into a single integer representing this same time in epoch seconds UTC.
 * @param {LocalDateTime} localDateTime the local date time value to convert.
 * @return {Integer} epoch second in UTC representing the given local date time.
 */
export function localDateTimeToEpochSecond(localDateTime) {
  const localDate = localDateTime.localDate;
  const localTime = localDateTime.localTime;

  const epochDay = dateToEpochDay(localDate);
  const localTimeSeconds = localTimeToSecondOfDay(localTime);
  return epochDay.multiply(SECONDS_PER_DAY).add(localTimeSeconds);
}

/**
 * Converts given epoch second and nanosecond adjustment into a local date time object.
 * @param {Integer|number|string} epochSecond the epoch second to use.
 * @param {Integer|number|string} nano the nanosecond to use.
 * @return {LocalDateTime} the local date time representing given epoch second and nano.
 */
export function epochSecondAndNanoToLocalDateTime(epochSecond, nano) {
  const epochDay = floorDiv(epochSecond, SECONDS_PER_DAY);
  const secondsOfDay = floorMod(epochSecond, SECONDS_PER_DAY);
  const nanoOfDay = secondsOfDay.multiply(NANOS_PER_SECOND).add(nano);

  const localDate = epochDayToDate(epochDay);
  const localTime = nanoOfDayToLocalTime(nanoOfDay);
  return new LocalDateTime(localDate, localTime);
}

/**
 * Converts given local date into a single integer representing it's epoch day.
 * @param {Date} date the date to convert.
 * @return {Integer} epoch day representing the given date.
 */
export function dateToEpochDay(date) {
  const year = int(date.year);
  const month = int(date.month);
  const day = int(date.day);

  let epochDay = year.multiply(365);

  if (year.greaterThanOrEqual(0)) {
    epochDay = epochDay.add(year.add(3).div(4).subtract(year.add(99).div(100)).add(year.add(399).div(400)));
  } else {
    epochDay = epochDay.subtract(year.div(-4).subtract(year.div(-100)).add(year.div(-400)));
  }

  epochDay = epochDay.add(month.multiply(367).subtract(362).div(12));
  epochDay = epochDay.add(day.subtract(1));
  if (month.greaterThan(2)) {
    epochDay = epochDay.subtract(1);
    if (!isLeapYear(year)) {
      epochDay = epochDay.subtract(1);
    }
  }
  return epochDay.subtract(DAYS_0000_TO_1970);
}

/**
 * Converts given epoch day to a local date.
 * @param {Integer|number|string} epochDay the epoch day to convert.
 * @return {Date} the date representing the epoch day in years, months and days.
 */
export function epochDayToDate(epochDay) {
  epochDay = int(epochDay);

  let zeroDay = epochDay.add(DAYS_0000_TO_1970).subtract(60);
  let adjust = int(0);
  if (zeroDay.lessThan(0)) {
    const adjustCycles = zeroDay.add(1).div(DAYS_PER_400_YEAR_CYCLE).subtract(1);
    adjust = adjustCycles.multiply(400);
    zeroDay = zeroDay.add(adjustCycles.multiply(-DAYS_PER_400_YEAR_CYCLE));
  }
  let year = zeroDay.multiply(400).add(591).div(DAYS_PER_400_YEAR_CYCLE);
  let dayOfYearEst = zeroDay.subtract(year.multiply(365).add(year.div(4)).subtract(year.div(100)).add(year.div(400)));
  if (dayOfYearEst.lessThan(0)) {
    year = year.subtract(1);
    dayOfYearEst = zeroDay.subtract(year.multiply(365).add(year.div(4)).subtract(year.div(100)).add(year.div(400)));
  }
  year = year.add(adjust);
  let marchDayOfYear = dayOfYearEst;

  const marchMonth = marchDayOfYear.multiply(5).add(2).div(153);
  const month = marchMonth.add(2).modulo(12).add(1);
  const day = marchDayOfYear.subtract(marchMonth.multiply(306).add(5).div(10)).add(1);
  year = year.add(marchMonth.div(10));

  return new Date(year, month, day);
}

/**
 * Format given duration to an ISO 8601 string.
 * @param {Integer|number} months the number of months.
 * @param {Integer|number} days the number of days.
 * @param {Integer|number} seconds the number of seconds.
 * @param {Integer|number} nanoseconds the number of nanoseconds.
 * @return {string} ISO string that represents given duration.
 */
export function durationToIsoString(months, days, seconds, nanoseconds) {
  const monthsString = formatNumber(months);
  const daysString = formatNumber(days);
  const secondsString = formatNumber(seconds);
  const nanosecondsString = formatNumber(nanoseconds, 9);
  return `P${monthsString}M${daysString}DT${secondsString}.${nanosecondsString}S`;
}

/**
 * Formats given time to an ISO 8601 string.
 * @param {Integer|number} hour the hour value.
 * @param {Integer|number} minute the minute value.
 * @param {Integer|number} second the second value.
 * @param {Integer|number} nanosecond the nanosecond value.
 * @return {string} ISO string that represents given time.
 */
export function timeToIsoString(hour, minute, second, nanosecond) {
  const hourString = formatNumber(hour, 2);
  const minuteString = formatNumber(minute, 2);
  const secondString = formatNumber(second, 2);
  const nanosecondString = formatNumber(nanosecond, 9);
  return `${hourString}:${minuteString}:${secondString}.${nanosecondString}`;
}

/**
 * Formats given time zone offset in seconds to string representation like '±HH:MM', '±HH:MM:SS' or 'Z' for UTC.
 * @param {Integer|number} offsetSeconds the offset in seconds.
 * @return {string} ISO string that represents given offset.
 */
export function timeZoneOffsetToIsoString(offsetSeconds) {
  offsetSeconds = int(offsetSeconds);
  if (offsetSeconds.equals(0)) {
    return 'Z';
  }

  const isNegative = offsetSeconds.isNegative();
  if (isNegative) {
    offsetSeconds = offsetSeconds.multiply(-1);
  }
  const signPrefix = isNegative ? '-' : '+';

  const hours = formatNumber(offsetSeconds.div(SECONDS_PER_HOUR), 2);
  const minutes = formatNumber(offsetSeconds.div(SECONDS_PER_MINUTE).modulo(MINUTES_PER_HOUR), 2);
  let secondsValue = offsetSeconds.modulo(SECONDS_PER_MINUTE);
  const seconds = secondsValue.equals(0) ? null : formatNumber(secondsValue, 2);

  return seconds ? `${signPrefix}${hours}:${minutes}:${seconds}` : `${signPrefix}${hours}:${minutes}`;
}

/**
 * Formats given date to an ISO 8601 string.
 * @param {Integer|number} year the date year.
 * @param {Integer|number} month the date month.
 * @param {Integer|number} day the date day.
 * @return {string} ISO string that represents given date.
 */
export function dateToIsoString(year, month, day) {
  year = int(year);
  const isNegative = year.isNegative();
  if (isNegative) {
    year = year.multiply(-1);
  }
  let yearString = year.toString().padStart(4, '0');
  if (isNegative) {
    yearString = '-' + yearString;
  }

  const monthString = formatNumber(month, 2);
  const dayString = formatNumber(day, 2);
  return `${yearString}-${monthString}-${dayString}`;
}

/**
 * Converts given local time into a single integer representing this same time in seconds of the day. Nanoseconds are skipped.
 * @param {LocalTime} localTime the time to convert.
 * @return {Integer} seconds representing the given local time.
 */
function localTimeToSecondOfDay(localTime) {
  const hour = int(localTime.hour);
  const minute = int(localTime.minute);
  const second = int(localTime.second);

  let totalSeconds = hour.multiply(SECONDS_PER_HOUR);
  totalSeconds = totalSeconds.add(minute.multiply(SECONDS_PER_MINUTE));
  return totalSeconds.add(second);
}


/**
 * Check if given year is a leap year. Uses algorithm described here {@link https://en.wikipedia.org/wiki/Leap_year#Algorithm}.
 * @param {Integer|number|string} year the year to check. Will be converted to {@link Integer} for all calculations.
 * @return {boolean} <code>true</code> if given year is a leap year, <code>false</code> otherwise.
 */
function isLeapYear(year) {
  year = int(year);

  if (!year.modulo(4).equals(0)) {
    return false;
  } else if (!year.modulo(100).equals(0)) {
    return true;
  } else if (!year.modulo(400).equals(0)) {
    return false;
  } else {
    return true;
  }
}

/**
 * @param {Integer|number|string} x the divident.
 * @param {Integer|number|string} y the divisor.
 * @return {Integer} the result.
 */
function floorDiv(x, y) {
  x = int(x);
  y = int(y);

  let result = x.div(y);
  if (x.isPositive() !== y.isPositive() && result.multiply(y).notEquals(x)) {
    result = result.subtract(1);
  }
  return result;
}

/**
 * @param {Integer|number|string} x the divident.
 * @param {Integer|number|string} y the divisor.
 * @return {Integer} the result.
 */
function floorMod(x, y) {
  x = int(x);
  y = int(y);

  return x.subtract(floorDiv(x, y).multiply(y));
}

/**
 * @param {Integer|number} num the number to format.
 * @param {number} [stringLength=undefined] the string length to left-pad to.
 * @return {string} formatted and possibly left-padded number as string.
 */
function formatNumber(num, stringLength = undefined) {
  const result = int(num).toString();
  return stringLength ? result.padStart(stringLength, '0') : result;
}