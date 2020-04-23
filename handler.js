'use strict';
const bent = require('bent');
const get = bent('string')
const AWS = require('aws-sdk');
const processData = require('./consolidate');
const {gzip} = require('node-gzip');
const population = require('population')
const bucket = 'website-jalrrb';
const jsuPath = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/";

const csv = {
    globCases: ["time_series_covid19_confirmed_global.csv", "cases.csv"],
    globDeaths: ["time_series_covid19_deaths_global.csv", "deaths.csv"],
    usCases: ["time_series_covid19_confirmed_US.csv", "cases_US.csv"],
    usDeaths: ["time_series_covid19_deaths_US.csv", "deaths_US.csv"],
}

module.exports.processCSV = async (event,context,callback) => {
    try {
        for (let prop in csv) {
            const data = await get(jsuPath + csv[prop][0]);
            await writeFile(data, csv[prop][1], );
            csv[prop] = data;
        }

        const [cases, pop, dates] = await processData(csv, population);
        const data = JSON.stringify({dates: dates, data: cases});
          await writeFile(data , "jhu.js", "application/javascript");
        return "OK"
    } catch (e) {
        return e.toString();
    }
    async function writeFile(data, key, type) {
        const s3 = new AWS.S3();
        const destparams = {
            Bucket: bucket,
            Key: key,
            Expires: new Date(new Date().getTime() + 1 * 60 * 1000),
            Body: (new Date()).toString() + "\n" + data,
            ContentType: type
        };
        await s3.putObject(destparams).promise();
    }
}
