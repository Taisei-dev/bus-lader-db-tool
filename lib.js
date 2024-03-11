import { PrismaClient } from '@prisma/client';
import fs from 'fs-extra';
import decompress from 'decompress';
import { parse as parseCsv } from 'csv-parse/sync';
import { parse as parseDate } from 'date-fns';
import constants from './constants.json' assert { type: 'json' };

const prisma = new PrismaClient();
const feeds = await prisma.feedInfo.findMany();

async function getGTFSData(companyId) {
  //get GTFS data
  let res = await fetch(constants[companyId].gtfsUrl);
  let arrayBuffer = await res.arrayBuffer();
  let buffer = Buffer.from(arrayBuffer);
  fs.emptyDirSync('tmp');
  fs.writeFileSync('tmp/gtfs.zip', buffer);
  await decompress('tmp/gtfs.zip', 'tmp/dist');
  res = arrayBuffer = buffer = null;
}

async function deleteOld(companyId) {
  //delete old
  await prisma.stopTime.deleteMany({
    where: {
      company_id: companyId,
    },
  });
  await prisma.trip.deleteMany({
    where: {
      company_id: companyId,
    },
  });
  await prisma.shapePoint?.deleteMany({
    where: {
      company_id: companyId,
    },
  });
  await prisma.shape?.deleteMany({
    where: {
      company_id: companyId,
    },
  });
  await prisma.route.deleteMany({
    where: {
      company_id: companyId,
    },
  });
}

async function updateRoutes(companyId) {
  //update routes
  let routeData = parseCsv(fs.readFileSync('tmp/dist/routes.txt'), {
    columns: true,
    bom: true,
  }).map((route) => {
    return {
      route_id: route['route_id'],
      company_id: companyId,
      short_name: route['route_short_name'],
      long_name: route['route_long_name'],
    };
  });
  await prisma.route.createMany({ data: routeData });
  routeData = null;
}

async function updateShapeAndShapePoints(companyId) {
  //update shapes and chapepoints
  let shapePointCSV = await parseCsv(fs.readFileSync('tmp/dist/shapes.txt'), {
    columns: true,
    bom: true,
  });
  let shapeIdData = shapePointCSV
    .filter((shapePoint) => shapePoint['shape_pt_sequence'] == 1)
    .map((shapePoint) => {
      return { shape_id: shapePoint['shape_id'], company_id: companyId };
    });
  await prisma.shape.createMany({
    data: shapeIdData,
  });

  let shapePointData = shapePointCSV.map((shapePoint) => {
    return {
      shape_id: shapePoint['shape_id'],
      company_id: companyId,
      shape_pt_sequence: Number(shapePoint['shape_pt_sequence']),
      shape_pt_lat: Number(shapePoint['shape_pt_lat']),
      shape_pt_lon: Number(shapePoint['shape_pt_lon']),
    };
  });
  shapePointCSV = shapeIdData = null;
  await prisma.shapePoint.createMany({
    data: shapePointData,
  });
  shapePointData = null;
}

async function updateTrips(companyId) {
  //update trips
  let tripData = parseCsv(fs.readFileSync('tmp/dist/trips.txt'), {
    columns: true,
    bom: true,
  }).map((trip) => {
    return trip['shape_id']
      ? {
          trip_id: trip['trip_id'],
          company_id: companyId,
          route_id: trip['route_id'],
          shape_id: trip['shape_id'],
        }
      : {
          trip_id: trip['trip_id'],
          company_id: companyId,
          route_id: trip['route_id'],
        };
  });
  await prisma.trip.createMany({ data: tripData });
  tripData = null;
}

async function updateTripsWithoutShape(companyId) {
  //update trips
  let tripData = parseCsv(fs.readFileSync('tmp/dist/trips.txt'), {
    columns: true,
    bom: true,
  }).map((trip) => {
    return {
      trip_id: trip['trip_id'],
      company_id: companyId,
      route_id: trip['route_id'],
    };
  });
  await prisma.trip.createMany({ data: tripData });
  tripData = null;
}

async function updateStopTimes(companyId) {
  //update stoptimes
  let stopsData = new parseCsv(fs.readFileSync('tmp/dist/stops.txt'), {
    columns: true,
    bom: true,
  }).map((stop) => {
    return [
      stop['stop_id'],
      {
        name: stop['stop_name'],
        lat: Number(stop['stop_lat']),
        lon: Number(stop['stop_lon']),
      },
    ];
  });
  let stopsMap = new Map(stopsData);
  stopsData = null;
  let stopTimeData = parseCsv(fs.readFileSync('tmp/dist/stop_times.txt'), {
    columns: true,
    bom: true,
  }).map((stoptime) => {
    let stop = stopsMap.get(stoptime['stop_id']);
    return {
      trip_id: stoptime['trip_id'],
      company_id: companyId,
      stop_sequence: Number(stoptime['stop_sequence']),
      arrival_time: stoptime['arrival_time'],
      departure_time: stoptime['departure_time'],
      stop_headsign: stoptime['stop_headsign'],
      stop_name: stop['name'],
      stop_lat: stop['lat'],
      stop_lon: stop['lon'],
    };
  });
  stopsMap = null;
  await prisma.stopTime.createMany({ data: stopTimeData });
  stopTimeData = null;
}

async function updateFeedInfo(companyId) {
  //update feed_info
  const feedEndDateString = parseCsv(
    fs.readFileSync('tmp/dist/feed_info.txt'),
    {
      columns: true,
      bom: true,
    }
  )[0]['feed_end_date'];
  const feedEndDate = parseDate(feedEndDateString, 'yyyyMMdd', new Date());
  await prisma.feedInfo.upsert({
    where: {
      company_id: companyId,
    },
    update: {
      feed_end_date: feedEndDate,
    },
    create: {
      company_id: companyId,
      company_name: constants[companyId].name,
      feed_end_date: feedEndDate,
    },
  });
}

export async function check() {
  let upToDate = [],
    updateNeeded = [];
  for (let companyId in constants) {
    //check if udpate needed
    const feed = feeds.find((feed) => feed.company_id == companyId);
    if (!feed) {
      updateNeeded.push([companyId, 'No data on database']);
    } else if (feed.feed_end_date < Date.now()) {
      updateNeeded.push([
        companyId,
        `Expired on ${feed.feed_end_date.toLocaleDateString()}`,
      ]);
    } else {
      upToDate.push(companyId);
    }
  }
  console.log('Up to date :');
  for (let companyId of upToDate) {
    console.log(`  ID ${companyId}  ${constants[companyId].name}`);
  }
  console.log('Update needed :');
  for (let [companyId, reason] of updateNeeded) {
    console.log(`  ID ${companyId}  ${constants[companyId].name}    ${reason}`);
  }
}

export async function updateOne(companyId) {
  //update
  console.log(`Working on : id ${companyId} ${constants[companyId].name}...`);
  try {
    await getGTFSData(companyId);
    await deleteOld(companyId);
    await updateRoutes(companyId);
    //shape.txt is optional
    if (fs.existsSync('tmp/dist/shapes.txt')) {
      await updateShapeAndShapePoints(companyId);
      await updateTrips(companyId);
    } else {
      await updateTripsWithoutShape(companyId);
    }
    await updateStopTimes(companyId);
    await updateFeedInfo(companyId);
    console.log('done.');
  } catch (e) {
    console.log('Error!\n', e);
  } finally {
    fs.removeSync('tmp');
  }
}

export async function updateAll() {
  for (let companyId in constants) {
    //check if udpate needed
    const feed = feeds.find((feed) => feed.company_id == companyId);
    if (feed && feed.feed_end_date > Date.now()) {
      console.log(`Up to date : id ${companyId} ${constants[companyId].name}.`);
      continue;
    }

    //update
    console.log(`Working on : id ${companyId} ${constants[companyId].name}...`);
    try {
      await getGTFSData(companyId);
      await deleteOld(companyId);
      await updateRoutes(companyId);
      //shape.txt is optional
      if (fs.existsSync('tmp/dist/shapes.txt')) {
        await updateShapeAndShapePoints(companyId);
        await updateTrips(companyId);
      } else {
        await updateTripsWithoutShape(companyId);
      }
      await updateStopTimes(companyId);
      await updateFeedInfo(companyId);
      console.log('done.');
    } catch (e) {
      console.log('Error!\n', e);
    } finally {
      fs.removeSync('tmp');
    }
  }
}
