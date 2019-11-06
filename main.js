/**
 * @author Dap "Naka" Tran
 * @github https://github.com/astablenaka
 * @website n/a
 */


const PDF2Pic = require('pdf2pic');
const tempDir = "./tools/butcher/temp";
const inDir = "./tools/lccps-spdf-rename/out";
const inDirCustom = "./tools/butcher/input";
const delDir = "./tools/butcher/deleted";
const outDir = "./tools/butcher/output";
const fs = require("fs");
const PDFDocument = require('pdfkit');
const gm = require("gm");
//- Modify pdf2png and add GM colors field to graphicMagickBaseCommand
//- To get rid of AA channels which cause larger file sizes
const dpi = 300;
const dpic = 300;
const blankThreshold = 20; // kb
const crypto = require('crypto');
const EventEmitter = require("events");

let events = null;
let fileQueue = [];
let wabuga = 0;
let nameGroups = {}

function loglog(...args){
	console.log(...args);
	if(events){
		events.emit('log', ...args);
	}
}

function logerror(...args){
	console.error(...args);
	if(events){
		events.emit('err', ...args);
	}
}

function loadFiles(onDone){
	return fs.readdirSync(inDir).mapNextTick((fileName)=>{
		let inpath = `${inDir}/${fileName}`;
		let file = createFileMetadata(fileName, inpath);
		let instat = fs.statSync( inpath );
		let insize = instat.size;
		file.size = insize;
		events.emit('fileLoaded', file);
		return file;
	}, onDone);
}

Array.prototype.mapNextTick = function(callback, onDone){
	let self = this;
	let index = 0;
	let length = self.length;
	let results = [];
	function doNext(){
		if(self[index]){
			results.push( callback( self[index], index, self ) );
			index++;
			setTimeout(doNext, 10);
		}else{
			onDone( results );
		}
	}
	doNext();
}

// Returns file
function createFileMetadata(fileName, source){
	return {
		name:fileName,
		source:source,
		groupDiscriminator:null,
		order: parseInt( fileName.match(/\d/gi)[0] )||1,
		dpi:dpi, dpic:dpi, blankThreshold:blankThreshold,
		checksum: null,
		size:0,
		pages:[]
	}
}

function createMetadata_v0(groupDiscriminator){
	return {
		groupDiscriminator: groupDiscriminator,
		checksum: null,
		pages:{
			total: 0,
			deleted: 0,
			kept: 0
		},
		size:{
			inputt:0,
			output:0,
			percent:0,
			delta:0
		},
		files:[],
	}
}

function createMetadata_v1(v0){
	v0.version = 1;
	v0.output = `${outDir}/${v0.groupDiscriminator}.pdf`;
	return v0;
}

function createMetadata_v2(v1){
	v1.version = 2;
	return v1;
}
function createMetadata_v3(v2){
	v2.version = 3;
	v2.forceBuild = true;
	v2.timeStart = 0;
	v2.timeEnd = 0;
	v2.time = 0;
	return v2;
}

function createMetadata_v4(v3){
	v3.version = 4;
	v3.forceBuild = true;
	v3.magnitude = 0;
	v3.expectedMagnitude = 0;
	v3.missing = false;
	return v3;
}

function createMetadata_v5(v4){
	v4.version = 5;
	v4.forceBuild = false;
	return v4;
}

function createMetadata_v6(v5){
	v5.version = 6;
	v5.forceBuild = false;
	return v5;
}

const META_VERSION = 6;
const upgradeStack = [ // Reserved for data restructuring
	createMetadata_v0,
	createMetadata_v1,
	createMetadata_v2,
	createMetadata_v3,
	createMetadata_v4,
	createMetadata_v5,
	createMetadata_v6
]

// This will upgrade the metadata
function upgradeMetadata( metadata, desiredVersion ){
	let version = metadata.version || 0;
	loglog(`[ Meta Upgrade ] v${version} -> v${desiredVersion}, ${metadata.groupDiscriminator}`);
	for( let cv = version+1; cv <= desiredVersion; cv++ ){
		upgradeStack[cv](metadata);
	}
}

function createMetadata(groupDiscriminator){
	const metadataPath = "./tools/butcher/metadata";
	if(fs.existsSync(`${metadataPath}/${groupDiscriminator}.meta.json`)){
		// This was supposed to update the meta file if said file was on an earlier version
		// but loading the metadata messes with the fileQueue file-to-group appending
		// routine.

		loglog(`[ MetaLoader ] loading ${metadataPath}/${groupDiscriminator}.meta.json`);
		let meta = JSON.parse(fs.readFileSync(`${metadataPath}/${groupDiscriminator}.meta.json`).toString());
		// Upgrade the metadata if it's not in the desired version
		if(meta.version != META_VERSION){
			loglog(`[ MetaLoader ] update for ${groupDiscriminator}`);
			upgradeMetadata( meta, META_VERSION );
			loglog(`[ MetaLoader ] saving update for ${groupDiscriminator}`);
			fs.writeFileSync(`${metadataPath}/${groupDiscriminator}.meta.json`, JSON.stringify(meta,null,"\t") );
		}
	}
	loglog(`[ Meta ] creation for ${groupDiscriminator}`);
	let meta = createMetadata_v0(groupDiscriminator);
	upgradeMetadata(meta, META_VERSION);
	events.emit('metaCreated', {
		groupDiscriminator:groupDiscriminator,
		version:META_VERSION,
		meta:meta
	})
	return meta;
}



function assignFilesToGroups(){
	fileQueue.map((file)=>{
		let fileName = file.name;
		let groupDiscriminator = fileName.replace(/[\s-]\d\.pdf/gi,'');
		nameGroups[groupDiscriminator] = nameGroups[groupDiscriminator] || createMetadata(groupDiscriminator);
		let group = nameGroups[groupDiscriminator];
		file.groupDiscriminator = groupDiscriminator;
	
		// If the file is a duplicate, don't do anything
		if(group.files.filter((p)=>{
			return p.name==file.name;
		}).length>0){
			return;
		}
	
		// Continuity check
		group.magnitude++;
		group.expectedMagnitude = (group.expectedMagnitude>file.order?group.magnitude:file.order);
		group.files.push( file );
	});
	events.emit('doneAssigningFilesToGroups', {
		groups:nameGroups,
		count:Object.keys(nameGroups).length
	})
}

function calculateFileChecksum(path){
	let hash = new crypto.createHash("sha256");
	let pdfFileBuffer = fs.readFileSync( path );
	hash.update(pdfFileBuffer);
	return hash.digest("hex");
}

function createPageData( page ){
	return {
		order:page,
		deleted:false
	}
}

//pdfParser.loadPDF(filedir, 5);
function butcherFile( file, order=0, clearTemp=true, doMerge=false, onDone=()=>{} ){
	let fileName = file.name;
	const discriminator = fileName.split('.')[0];
	const savedir = `${tempDir}`;
	const doc = new PDFDocument({autoFirstPage:false});
	let group = nameGroups[file.groupDiscriminator];
	loglog(group);
	
	if(!fs.existsSync(savedir)){
		fs.mkdirSync( savedir );
	}
	const converter = new PDF2Pic({
		density:dpi,
		savedir:savedir,
		format:"png",
		size:`${8.5*dpic}x${11*dpic}`,
		savename:`${discriminator}.o.${order}`
	});

	events.emit('beforeButcherFile', {
		file:file, order:order, clearTemp:clearTemp, doMerge:doMerge, onDone:onDone
	})

	function clear(){
		if(!clearTemp){return;}
		const files = fs.readdirSync(`${savedir}`);
		files.map( (fileName)=>{
			let path = `${savedir}/${fileName}`;
			let stat = fs.statSync( path );
			let size = stat.size/1024;
			fs.unlinkSync(path);
		} )
	}
	

	const startTime = new Date().getTime();
	let dl = new Date().getTime();
	function produce(page){
		const inputPath = `${inDir}/${fileName}`;
		file.checksum = calculateFileChecksum(inputPath);
		converter.convertBulk( inputPath, [page]).then((resolve)=>{
				loglog(`[ Page ] ${page} ${(new Date().getTime() - dl)/1000} S`);
				dl = new Date().getTime();
				file.pages.push( createPageData( page ) );
				group.pages.total++;
				// 100 ms cooldown for no hangs
				setTimeout(()=>{
					produce(page+1);
				},100)
				return resolve;
		}).catch((e)=>{
			loglog(`${Math.floor((new Date().getTime() - startTime)/1000)} seconds to produce ${page-1} pages`);
			reduce();
		});
	}

	function dunnessWrapper(){
		events.emit('doneButcherFile', {
			file:file, order:order, clearTemp:clearTemp, doMerge:doMerge, onDone:onDone
		})
		onDone();
	}
	
	function merge(){
		const outputPath = `${outDir}/${file.groupDiscriminator}.pdf`;
		doc.pipe(fs.createWriteStream(outputPath), {
			autoFirstPage:false
		});
		const files = fs.readdirSync(`${savedir}`);
		let sorted = files.sort((a,b)=>{
			//loglog(parseInt(a.split('_')[1]||0)+order*1000, parseInt(b.split('_')[1]||0)+order*1000);
			return parseInt(a.split('_')[1]||0)+order*1000-parseInt(b.split('_')[1]||0)+order*1000;
		});
		loglog(sorted);
		let sizes = [];
		let gmsum = 0;
		sorted.map((pagename,i)=>{
			gm(`${savedir}/${pagename}`).size((e,size)=>{
				sizes[i] = size;
				gmsum++;
				if(gmsum==sorted.length){
					composite();
				}
			})
		})

		function composite(){
			loglog(`[ Composite ] ${discriminator}`)
			sorted.map((pagename,i)=>{
				let size = sizes[i];
				let width = size.width/dpic*72;
				let height = size.height/dpic*72;
				doc.addPage({size:[width,height]});
				doc.image(`${savedir}/${pagename}`,0,0,{width:width,height:height});
				loglog(`[ Page ] ${pagename}`);
			})
			doc.end();
			let cooldown = 10000+group.files.length*2000;
			loglog(`[ Cooldown ] ${cooldown/1000} Seconds...`);
			setTimeout(()=>{
				let outpath = outputPath;
				let outstat = fs.statSync( outpath );
				let outsize = outstat.size;
				group.size.output = outsize;
				group.size.inputt = group.files.map((f)=>{return f.size}).reduce((a,b)=>{return a+b;});
				group.size.percent = (100*group.size.output)/group.size.inputt;
				group.size.delta = group.size.output-group.size.inputt
				group.checksum = calculateFileChecksum(outputPath);
				loglog(`[ Finalizing ] ...`);
				dunnessWrapper();
			}, cooldown);

		}
	}

	function reduce(){
		const files = fs.readdirSync(`${savedir}`);
		let delsum = files.map( (fileName)=>{
			let path = `${savedir}/${fileName}`;
			let stat = fs.statSync( path );
			let size = stat.size/1024;
			if(size<blankThreshold){
				let order = parseInt( (fileName.match(/\.o\.\d/gi)[0]||'').split('.')[2]||0 )-1;
				let index = parseInt( (fileName.match(/_\d{1,}/gi)[0]||'').split('_')[1]||0 ) - 1;
				loglog(order, index)
				group.files[order].pages[index].deleted = true;
				group.pages.deleted++;
				//fs.renameSync(path, `${delDir}/${fileName}`);
				fs.unlinkSync(path);
				events.emit('unlinkBlank', {
					path:path,
					group:group,
					file:file,
					size:size,
					threshold:blankThreshold
				})
				return 1;
			}
			return 0;
		} ).reduce((p,c)=>p+c);
		loglog(`[ Butchered ] ${delsum} pages.`);
		if(doMerge){
			group.pages.kept = group.pages.total-group.pages.deleted;
			merge();
		}else{
			dunnessWrapper();
		}
		
	}
	clear();
	produce(1);
}

let groupList = null; 
function processGroup( groupIndex, fileIndex=0 ){
	if(!groupList){
		groupList = Object.values(nameGroups);
	}
	
	let group = groupList[groupIndex];
	let needsRebuilding = false||(group||{}).forceBuild;
	if(!group){
		loglog("[ No Groups Left ]");
		events.emit("doneProcessing");
		return;
	}
	const metaPath = `./tools/butcher/metadata/${group.groupDiscriminator}.meta.json`;
	if(fileIndex==0){
		group.timeStart = new Date().getTime();
	}
	// Check checksums for differences before processing
	if(fs.existsSync(metaPath)){
		let oldGroupData = JSON.parse( fs.readFileSync(metaPath).toString() );
		loglog(`[ Metadata ] found for ${group.groupDiscriminator}`)
		group.files.map( (file, i)=>{
			loglog(i);
			let source = file.source || `${inDir}/${file.name}`;
			let oldChecksum = (oldGroupData.files[i]||{}).checksum||0x80;
			let checksum = calculateFileChecksum( source );
			let condition = checksum!=oldChecksum;
			loglog(`[ OLD ] ${ oldChecksum }`);
			if( condition ){
				needsRebuilding = true;
			}
			console[condition ? "warn" : "log"](`[ NEW ] ${ checksum } ( ${ condition ? 'Different' : 'Match' } )`);
		})

		// Rebuild if the file is deleted, (TODO) unless specified no-rebuild
		if(!fs.existsSync(`${outDir}/${oldGroupData.groupDiscriminator}.pdf`)){
			needsRebuilding = true;
		}
	}else{ needsRebuilding = true; }

	// If continuity breaks
	if(group.magnitude!=group.expectedMagnitude){
		group.missing = true;
		logerror(`[ DISCONTINUOUS ] ${group.groupDiscriminator}, skipping!`);
		needsRebuilding = false;
		events.emit('foundDiscontinuous', {
			group:group
		})
		save();
	}

	function save(){
		group.timeEnd = new Date().getTime();
		group.time = group.timeEnd - group.timeStart;
		group.forceBuild = false;
		fs.writeFile(metaPath, JSON.stringify(group, null, "\t"), (e)=>{
			if(e)
				throw e;
			loglog(`[ Metadata ] for ${group.groupDiscriminator} saved`);
			cleanup();
		});
	}

	function cleanup(){
		// Cleanup
		nameGroups[group.groupDiscriminator] = null;
		groupList[groupIndex] = null;
	}

	events.emit("processGroup", {group:group,build:needsRebuilding})
	
	if(!needsRebuilding){
		loglog(`[ Rebuild Not Required ] for ${group.groupDiscriminator}`);
		cleanup();
		setTimeout(()=>{
			processGroup(groupIndex+1, 0);
		},10)
		return;
	}

	loglog(`[ Building ] group "${group.groupDiscriminator}"`);
	loglog(group);
	if(group.files[fileIndex]){
		let file = group.files[fileIndex];
		loglog(`[ Starting File ] ${file.name}`);
		loglog(file),
		butcherFile( file, file.order, fileIndex==0, fileIndex==group.files.length-1, ()=>{
			
			setTimeout(()=>{
				processGroup( groupIndex, fileIndex+1 );
			},10)
		} )
	}else{
		save();
		events.emit("doneProcessGroup", {group:group,build:needsRebuilding})
		setTimeout(()=>{
			processGroup(groupIndex+1, 0);
		}, 250);
	}
}

function doStuff(ee){
	loglog("Doing Stuff!")
	events = ee||new EventEmitter();
	loadFiles((results)=>{
		fileQueue = results;
		assignFilesToGroups();
		return setTimeout(()=>{
			processGroup(0);
		}, 1000);
	});
}

module.exports = doStuff;
//doStuff();

//module.exports.start = doStuff;