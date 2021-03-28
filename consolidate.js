const fs = require('fs').promises;
const {gzip, ungzip} = require('node-gzip');
const ccs = require('./cc.js');
const cc = require('./iso23.js');
const st = require('./states.js');

module.exports = async function processData (csv, population) {
    processCSV(csv);
    const cc = getCountryCodes();
    population = population || getPopulation(csv.usPop, csv.globPop);
    addSumsUS(csv);
    let [cases, dr1] = getUSCases(csv.usCases, undefined, "cases", population, true, cc);
    let [c2, dr2] = getUSCases(csv.usDeaths, cases, "deaths", population, false, cc)
    addSumsGlob(csv);
    let [c3, dr3] = getGlobCases(csv.globCases, cases, "cases", population, cc);
    let [c4, dr4] = getGlobCases(csv.globDeaths, cases, "deaths", population, cc);
    getVaccinations(csv.vaccinations, cases, dr1);
    getUsVaccinations(csv.usVaccinations, cases, dr1)
    getGlobTests(csv.globTests, cases, dr1);
    //getUSTests(csv.usTests, cases, dr1);
    getUSTestsCDC(csv.usTestsCDC, cases, dr1);

    if (dr1.toString() !== dr2.toString() || dr1.toString() !== dr3.toString() || dr1.toString() !== dr4.toString()) {
        console.log(dr1);
        console.log(dr2);
        console.log(dr3);
        console.log(dr4);
        throw ("inconsistent data ranges");
    }
    return [shrink(cases), population, dr1];
}
function shrink(cases) {
    for (const c in cases)
        for (const p in cases[c])
            if (cases[c][p] instanceof Array)
                cases[c][p] = compress(cases[c][p]);
    return cases;
    function compress (a) {
        const nr = [];
        let nz = false;
        a.map(v => {
            v = v * 1;
            if (v > 0)
                nz = true;
            if (nz)
                nr.push(v);
        })
        return nr;
    }
}
function processCSV(csv, dateRange) {
    console.log("File Lengths");
    Object.getOwnPropertyNames(csv).map( f => {

        csv[f] = csv[f].split("\n").map(line =>
            line.replace(/;/g,'')
                .replace(/\r/,'')
                .replace(/\"(.*?)\"/g,(s)=>
                    s.replace(/,/g, '~').replace(/"/g, ""))
                .replace(/,/g,";")
                .replace(/~/g, ',')
                .split(";")
        );
    });
    return csv;
}
function getUsVaccinations(file, location, dr) {
    const dayMS = 1000 * 60 * 60 * 24;
    const props=getProps(file[0]);
    const countryData = {}
    const toDate = new Date(dr[dr.length - 1]).getTime();
    const fromDate = new Date(dr[0]).getTime();
    const days = Math.round((toDate - fromDate) / dayMS) + 1;
    let currentCountry = null;
    file.slice(1).map(line => {

        const country = (line[props['location']] + "").replace(/ State/, '');
        currentCountry = country;
        const date = new Date(line[props['date']]).getTime();
        const vaccinated = line[props['people_vaccinated']]*1;
        if (!countryData[country])
            countryData[country] = new Array(days).fill(0);
        if (date < fromDate || date > toDate)
            return;
        countryData[country][Math.round((date - fromDate) / dayMS)] = vaccinated;
    });
    for (let country in location) {
        const locationData = location[country];
        if (locationData.type !== 'state')
            continue;
        const vaccinations = countryData[locationData.name];
        if (vaccinations)
            locationData.vaccinations = fillout(vaccinations);
        else
            locationData.vaccinations = new Array(days).fill(0);
    }
}
function fillout(arr) {
    let last = 0;
    last = 0;
    lastActual = 0;
    let increment = 0;
    arr.map((count, ix) => {
        if (count) {
            increment = 0;
            lastActual = count;
            last = count;
        } else {
            if(!increment)
                arr.slice(ix).findIndex((c, ix) => {
                    if (c > 0)
                        increment = Math.floor((c - lastActual) / (ix + 1));
                    return c
                })
        }
        arr[ix] = count || (lastActual ? (last + increment) : count);
        last = last + increment;
    })
    return arr;
}
function getVaccinations(file, location, dr) {
    const dayMS = 1000 * 60 * 60 * 24;
    const props=getProps(file[0]);
    const countryData = {}
    const toDate = new Date(dr[dr.length - 1]).getTime();
    const fromDate = new Date(dr[0]).getTime();
    const days = Math.round((toDate - fromDate) / dayMS) + 1;
    let currentCountry = null;
    file.slice(1).map(line => {

        const country = line[props['iso_code']];
        currentCountry = country;
        const date = new Date(line[props['date']]).getTime();
        const vaccinated = line[props['people_vaccinated']]*1;
        if (!countryData[country])
            countryData[country] = new Array(days).fill(0);
        if (date < fromDate || date > toDate)
            return;
        countryData[country][Math.round((date - fromDate) / dayMS)] = vaccinated;
    });
    for (let country in location) {
        const locationData = location[country];
        if (locationData.type !== 'country' && locationData.type !== 'N/A')
            continue;
        const vaccinations = countryData[country === 'Total' ? 'OWID_WRL' : cc.cc[locationData.code]];
        if (vaccinations) {
            const f = fillout(vaccinations);
            locationData.vaccinations = fillout(vaccinations);
        } else
            locationData.vaccinations = new Array(days).fill(0);
    }
}
function getGlobTests(file, location,dr) {
    const dayMS = 1000 * 60 * 60 * 24;
    const props=getProps(file[0]);
    const countryData = {}
    const toDate = new Date(dr[dr.length - 1]).getTime();
    const fromDate = new Date(dr[0]).getTime();
    const days = Math.round((toDate - fromDate) / dayMS) + 1;

    file.slice(1).map(line => {
        const country = line[props['ISO code']];
        const date = new Date(line[props['Date']]).getTime();
        const Entity = line[props['Entity']];
        const type = Entity.split(" - ")[1];
        const cumulative = line[props['Cumulative total']] * 1;
        const daily = line[props['Daily change in cumulative total']] * 1;
        if (!countryData[country])
            countryData[country] = new Array(days).fill(0);
        if (date < fromDate || date > toDate)
            return;
        countryData[country][Math.round((date - fromDate) / dayMS)] =
            countryData[country][Math.round((date - fromDate) / dayMS)] || {};
        const data = countryData[country][Math.round((date - fromDate) / dayMS)];
        const previousData = countryData[country][Math.round((date - fromDate) / dayMS) - 1];
        if (cumulative > 0)
            data[type] = cumulative
        else if (daily > 0)
            data[type] = (previousData ? previousData[type] : 0) + daily
        else
            data[type] = (previousData ? previousData[type] : 0);
    });
    for (let country in location) {
        const locationData = location[country];
        if (locationData.type !== 'country')
            continue;
        let testData = countryData[cc.cc[locationData.code]];
        if (!testData) {
            console.log(`No test data for ${country}`)
            continue;
        }
        locationData.tests = new Array(days).fill(0);
        let total = 0;
        for (let ix = 0; ix < locationData.tests.length; ++ix) {
            if (testData[ix]) {
                total = 0;
                for (let type in testData[ix])
                    total = Math.max(total, testData[ix][type])
                }
            locationData.tests[ix] = total;
        }
    };
}
function getUSTestsCDC(file, location,dr) {
    const dayMS = 1000 * 60 * 60 * 24;
    const props=getProps(file[0]);
    const countryData = {}
    const toDate = new Date(dr[dr.length - 1]).getTime();
    const fromDate = new Date(dr[0]).getTime();
    const days = Math.round((toDate - fromDate) / dayMS) + 1;

    file.slice(1).map(line => {
        const country = st.stateAbbreviations[line[0]];
        const date = new Date(line[props['date']]).getTime();
        const count = (line[props['total_results_reported']] + "").replace(/,/g, '') * 1;
        if (!countryData[country])
            countryData[country] = new Array(days).fill(0);
        if (date < fromDate || date > toDate)
            return;
        countryData[country][Math.round((date - fromDate) / dayMS)] += count;
    });
    for (let country in location) {
        const locationData = location[country];
        if (locationData.type !== 'state')
            continue;
        let testData = countryData[country];
        if (!testData) {
            console.log(`No test data for ${country}`)
            continue;
        }
        locationData.tests = testData;
        for (let ix = 0; ix < locationData.tests.length; ++ix)
            if (locationData.tests[ix] === 0 && ix > 0 && locationData.tests[ix - 1] > 0)
                locationData.tests[ix] = locationData.tests[ix - 1]

    };
    let usTests = new Array(days).fill(0);
    const countryDataArray = Object.keys(countryData).map(k => countryData[k]);
    location["United States"].tests = usTests.map((v, ix) => countryDataArray.reduce((a, v) => {
        return a + v[ix]*1
    }, 0));
    console.log(location["United States"].tests.length);
    function fixDate(date) {
        date = date + "";
        return date[4]+date[5]+"/"+date[6]+date[7]+"/"+date[0]+date[1]+date[2]+date[3];
    }
}

function getUSTests(file, location,dr) {
    const dayMS = 1000 * 60 * 60 * 24;
    const props=getProps(file[0]);
    const countryData = {}
    const toDate = new Date(dr[dr.length - 1]).getTime();
    const fromDate = new Date(dr[0]).getTime();
    const days = Math.round((toDate - fromDate) / dayMS) + 1;

    file.slice(1).map(line => {
        const country = st.stateAbbreviations[line[props['state']]];
        const date = new Date(fixDate(line[props['date']])).getTime();
        const count = line[props['positive']] * 1 + line[props['negative']] * 1;
        if (!countryData[country])
            countryData[country] = new Array(days).fill(0);;
        if (date < fromDate || date > toDate)
            return;
        countryData[country][Math.round((date - fromDate) / dayMS)] = count;
    });
    for (let country in location) {
        const locationData = location[country];
        if (locationData.type !== 'state')
            continue;
        let testData = countryData[country];
        if (!testData) {
            console.log(`No test data for ${country}`)
            continue;
        }
        locationData.tests = testData;
        for (let ix = 0; ix < locationData.tests.length; ++ix)
            if (locationData.tests[ix] === 0 && ix > 0 && locationData.tests[ix - 1] > 0)
                locationData.tests[ix] = locationData.tests[ix - 1]

    };
    let usTests = new Array(days).fill(0);
    const countryDataArray = Object.keys(countryData).map(k => countryData[k]);
    location["United States"].tests = usTests.map((v, ix) => countryDataArray.reduce((a, v) => {
        return a + v[ix]*1
    }, 0));
    console.log(location["United States"].tests.length);
    function fixDate(date) {
        date = date + "";
        return date[4]+date[5]+"/"+date[6]+date[7]+"/"+date[0]+date[1]+date[2]+date[3];
    }
}
function getUSCases(file, location, prop, populationData, isCases) {
    const props=getProps(file[0]);
    const dates = Object.getOwnPropertyNames(props).filter((p)=>p.match(/\d*\/\d*\/\d*/));
    location = location || {};
    file.slice(1).map( line => {
        if (line[props.Combined_Key]) {
            const county = line[props['Admin2']];
            const state = line[props['Province_State']]
            const key = county ? county + ", " + state : state;
            const dataProp = isCases ? 11 : 12;
            if (!key.match(/^Out of/i) && !key.match(/^Unassigned/i)) {
                location [key] = {
                    name: key,
                    code: 'US',
                    type: key.match(/,/) ? "county" : "state",
                    longitude: line[props.Long_],
                    latitude: line[props.Lat],
                    population: location[key] ? location [key].population : 0,
                    cases: location[key] ? location [key].cases : [],
                    deaths: location[key] ? location [key].deaths : [],
                }
                location [key][prop] = line.slice(dataProp, line.length)
                if (!isCases) {
                    location[key]['population'] = line[dataProp - 1];
                    if (line[dataProp - 1]*1 === 0)
                        delete location[key];
                }
            }
        }
    });
    return [location, dates];
}
const countryCorrections = {
    "Cote d'Ivoire": "Côte d'Ivoire",
    "\"Korea": "South Korea",
    "Korea": "South Korea",
    "Korea, South": "South Korea",
    "US": "United States",
    "Taiwan*": "Taiwan",
    "Brunei" : "Brunei Darussalam",
    "Vietnam" : "Viet Nam",
    "Korea South": "South Korea",
    "Georgia" : "Georgia (Sakartvelo)",
    "Falkland Islands (Malvinas)": "Falkland Islands",
    "Turks and Caicos Islands": "Turks and Caicos"
}
const populationCorrections = {
    "Taiwan Province of China" : "Taiwan",
    "United States of America" : "United States",
    "United States Virgin Islands" : "Virgin Islands",
    "Bolivia (Plurinational State of)" : "Bolivia",
    "Iran (Islamic Republic of)" : "Iran",
    "Republic of Korea" : "South Korea",
    "Republic of Moldova": "Moldova",
    "Russian Federation" : "Russia",
    "United Republic of Tanzania": "Tanzania",
    "Venezuela (Bolivarian Republic of)": "Venezuela",
    "Syrian Arab Republic": "Syria",
    "Lao People's Democratic Republic": "Laos",
    "Georgia" : "Georgia (Sakartvelo)"

}
function getCountryCodes() {
    const cc = {};
    ccs.cc.map( c => cc[correct(c.name)] = c['alpha-2']);
    return cc;
    function correct(c) {
        return populationCorrections[c] || c;
    }
}
function getGlobCases(file, location, prop, populationData, cc) {
    const props=getProps(file[0])
    const dates = Object.getOwnPropertyNames(props).filter((p)=>p.match(/\d*\/\d*\/\d*/));
    location = location || {};
    file.slice(1).map( line => {
        let key = countryCorrections[line[props['Country/Region']]] || line[props['Country/Region']];
        let population = populationData[key];
        const province = line[props['Province/State']];
        let type = "country";
        let countryCode = cc[key];

        if (province)
            if (populationData[province]) {
                population = populationData[province];
                if (key === "United Kingdom") {
                    countryCode = cc[province]
                    key = countryCorrections[province] || province;
                    type = "country";
                } else {
                    key = province.match(key) ? province : province + ", " + key;
                    type = "province";
                }
            } else
                if (!key.match(/China/))
                    console.log("No population data for " + province + " " + key);

        if (key) {
            if (!population ) {
                    console.log("No population data for " + key);
            } else {
                if (!countryCode)
                    console.log("Missing country code for " + key);
                if (location[key] && location[key].type !== type)
                    console.log("Duplicate Country " + key);
                location [key] = {
                    name: key,
                    code: countryCode || "N/A",
                    longitude: line[props.Long],
                    latitude: line[props.Lat],
                    type: type,
                    population: population,
                    cases: location[key] ? location [key].cases : [],
                    deaths: location[key] ? location [key].deaths : [],
                }
                location [key][prop] = line.slice(4, line.length)
            }
        }
    });
    return [location, dates];
}
function getPopulation(us, glob) {
    const usProps = {county: 0, state: 1, population: 13}
    const globProps = getProps(glob[0]);
    console.log("US Columns");
    Object.getOwnPropertyNames(usProps).map( p => console.log(`${p}: ${usProps[p]}`));
    console.log("Global Columns");
    Object.getOwnPropertyNames(globProps).map( p => console.log(`${p}: ${globProps[p]}`));

    let population = {
        "Total": 7777439049,
        "Congo (Brazzaville)": 1800000,
        "Congo (Kinshasa)": 11860000,
        "Taiwan": 23780000,
        "West Bank and Gaza": 4685000,
        "Kosovo": 1831000,
        "Burma": 53370000,
        "Ontario": 14446515,
        "Quebec": 8433301,
        "British Columbia": 5020302,
        "Alberta": 4345737,
        "Manitoba": 1360396,
        "Saskatchewan": 1168423,
        "Nova Scotia": 965382,
        "New Brunswick": 772094,
        "Newfoundland and Labrador": 523790,
        "Prince Edward Island": 154748,
        "Northwest Territories": 44598,
        "Yukon": 40369,
        "Nunavut": 38787,
        "South Australia": 1659800,
        "Western Australia": 2366900,
        "Queensland": 4599400,
        "Victoria": 5640900,
        "New South Wales": 7317500,
        "Australian Capital Territory": 428060,
        "Northern Territory": 428060,
        "Tasmania": 515000,

    };
    let states = {}
    glob.slice(1).filter(l => l[globProps.Time] === '2020')
        .map(l => population[populationCorrections[l[globProps.Location]] || l[globProps.Location]] = l[globProps.PopTotal]*1000);
    return population;

    function fixUSState(state) {
        return state.substr(1)
            .replace(/^ /, '')
            .replace(/^\./, '')
    }
    function fixUSNames(county, state) {
        return county.substr(1)
            .replace(/ County/i, '')
            .replace(/^ /, '')
            .replace(/^\./, '')
            .replace(/ City and Borough/i, '')
            .replace(/Doña/, "Dona")
            .replace( / Borough/i, '')
            .replace( / City/i, '')
            .replace(/ Census Area/i, '')
            .replace( / Municipality/i, '')
            .replace(/ Parish/i, '') + "," + state;
    }
}
function getProps(headerLine) {
    let props = {};
    headerLine.map( (p, i) => props[p] = i)
    return props;
}
let sumsProvinces;
let countriesWithTotals = {};

function addSumsUS(csv) {

    const props = getProps(csv.usCases[0]);

    sumsProvinces = {};
    countriesWithTotals = {};
    csv.usCases.slice(1).map(line => sumStates(line, props, true));
    csv.usCases = csv.usCases.concat(totalsFromStates());

    sumsProvinces = {};
    countriesWithTotals = {};
    csv.usDeaths.slice(1).map(line => sumStates(line, props, false));
    csv.usDeaths = csv.usDeaths.concat(totalsFromStates());
}
function addSumsGlob (csv) {
    const props = getProps(csv.globCases[0]);

    sumsProvinces = {};
    countriesWithTotals = {};
    csv.globCases.slice(1).map(line => sumProvinces(line, props));
    csv.globCases.slice(1).map(line => sumWorld(line, props));
    csv.globCases = csv.globCases.concat(totalsFromProvinces());

    sumsProvinces = {};
    countriesWithTotals = {};
    csv.globDeaths.slice(1).map(line => sumProvinces(line, props));
    csv.globDeaths.slice(1).map(line => sumWorld(line, props));
    csv.globDeaths = csv.globDeaths.concat(totalsFromProvinces());
}
const theWorld = 'Total';
function sumStates (line, props, isCases) {
    if (!line[props['Admin2']]) {
        countriesWithTotals[line[props['Admin2']]] = true;
        return line;
    }

    const dataProp = isCases ? 11 : 12;
    const newLine = line.slice(0, props.Combined_Key).concat([line[props['Province_State']] + ', US'])
    if (!isCases)
        newLine.push(0)
    newLine[props['Admin2']] = '';


    sumsProvinces[line[props['Province_State']]] = sumsProvinces[line[props['Province_State']]] || {
        series: [], line: newLine,
    }
    const sum = sumsProvinces[line[props['Province_State']]];
    const series = line.slice(dataProp, line.length).map(point=>point * 1);
    series.map((point, ix) => sum.series[ix] = (sum.series[ix] || 0) + point * 1) ;
    if (!isCases)
        sum.line[dataProp - 1] += line[dataProp - 1] * 1;
    return line;
}
function totalsFromStates () {
    let additionalLines = [];
    Object.getOwnPropertyNames(sumsProvinces).map(province => {
        const sum = sumsProvinces[province];
        if (!countriesWithTotals[province])
            additionalLines.push(sum.line.slice(0, sum.dataProp).concat(sum.series))
    });
    return additionalLines;
}
function sumProvinces (line, props) {
    if (!line[props['Province/State']]) {
        countriesWithTotals[line[props['Country/Region']]] = true;
        return line;
    }
    sumsProvinces[line[props['Country/Region']]] = sumsProvinces[line[props['Country/Region']]] || {
        series: [], line: line,
    }
    const sum = sumsProvinces[line[props['Country/Region']]];
    const series = line.slice(4, line.length).map(point=>point * 1);
    series.map((point, ix) => sum.series[ix] = (sum.series[ix] || 0) + point * 1) ;

    return line;
}
function totalsFromProvinces () {
    let additionalLines = [];
    Object.getOwnPropertyNames(sumsProvinces).map(province => {
        const sum = sumsProvinces[province];
        if (!countriesWithTotals[province])
            additionalLines.push(['', sum.line[1], sum.line[2], sum.line[3]].concat(sum.series))
    });
    return additionalLines;
}
function sumWorld (line, props) {
    if (line[props['Province/State']])
        return line;
    sumsProvinces[theWorld] = sumsProvinces[theWorld] || {
        series: [], line: ['', theWorld, 0, 0],
    }
    const sum = sumsProvinces[theWorld];
    const series = line.slice(4, line.length).map(point=>point * 1);
    series.map((point, ix) => sum.series[ix] = (sum.series[ix] || 0) + point * 1);
    //console.log(line[props['Country/Region']] + ":" + series[series.length - 1] + " " + sum.series[sum.series.length - 1]);
    return line;
}
