#include "llvm/Support/CommandLine.h"

#include "mlir/Dialect/Arith/IR/Arith.h"
#include "mlir/Dialect/DB/IR/DBDialect.h"
#include "mlir/Dialect/DSA/IR/DSADialect.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/Dialect/LLVMIR/LLVMDialect.h"
#include "mlir/Dialect/MemRef/IR/MemRef.h"
#include "mlir/Dialect/RelAlg/IR/RelAlgDialect.h"
#include "mlir/Dialect/SubOperator/SubOperatorDialect.h"
#include "mlir/Dialect/TupleStream/TupleStreamDialect.h"

#include "mlir/Dialect/SCF/IR/SCF.h"
#include "mlir/Dialect/util/UtilDialect.h"

#include "mlir/AsmParser/AsmParser.h"
#include "mlir/AsmParser/AsmParserState.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Dialect.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Transforms/GreedyPatternRewriteDriver.h"
#include <llvm/ADT/TypeSwitch.h>
#include <llvm/Support/ErrorOr.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/SourceMgr.h>

#include <iostream>
#include <list>
#include <queue>

#include "json.h"
namespace {
mlir::Location dropNames(mlir::Location l) {
   if (auto namedLoc = mlir::dyn_cast<mlir::NameLoc>(l)) {
      return dropNames(namedLoc.getChildLoc());
   }
   return l;
}
} // end namespace
int main(int argc, char** argv) {
   mlir::DialectRegistry registry;
   registry.insert<mlir::relalg::RelAlgDialect>();
   registry.insert<mlir::tuples::TupleStreamDialect>();
   registry.insert<mlir::subop::SubOperatorDialect>();
   registry.insert<mlir::db::DBDialect>();
   registry.insert<mlir::dsa::DSADialect>();
   registry.insert<mlir::func::FuncDialect>();
   registry.insert<mlir::util::UtilDialect>();
   registry.insert<mlir::arith::ArithDialect>();

   registry.insert<mlir::scf::SCFDialect>();

   registry.insert<mlir::util::UtilDialect>();
   registry.insert<mlir::LLVM::LLVMDialect>();
   registry.insert<mlir::memref::MemRefDialect>();
   size_t opId = 0;
   std::unordered_map<std::string, size_t> analyzedOps;
   nlohmann::json j;

   for (int param = 1; param < argc; param++) {
      mlir::MLIRContext context;
      context.appendDialectRegistry(registry);
      auto inputFilename = std::string(argv[param]);

      llvm::SourceMgr sourceMgr;
      auto fileOrErr = llvm::MemoryBuffer::getFileOrSTDIN(inputFilename);
      if (std::error_code error = fileOrErr.getError())
         llvm::errs() << "could not open input file " + inputFilename << "\n";

      // Load the MLIR source file.
      sourceMgr.AddNewSourceBuffer(std::move(*fileOrErr), llvm::SMLoc());
      mlir::Block block;
      mlir::AsmParserState state;
      if (mlir::parseAsmSourceFile(sourceMgr, &block, mlir::ParserConfig(&context), &state).failed()) {
         llvm::errs() << "Error can't load file " << inputFilename << "\n";
         return 3;
      }
      llvm::DenseMap<mlir::Operation*, size_t> opIds;
      block.walk<mlir::WalkOrder::PreOrder>([&](mlir::Operation* op) {
         const auto* opDef = state.getOpDef(op);
         if (opDef) {
            auto loc1 = sourceMgr.getLineAndColumn(opDef->scopeLoc.Start);
            nlohmann::json operation;
            operation["id"] = opId++;
            opIds[op] = operation["id"];
            operation["representation"] = std::string(opDef->scopeLoc.Start.getPointer(), opDef->scopeLoc.End.getPointer());
            operation["loc"] = inputFilename + ":" + std::to_string(loc1.first);
            analyzedOps[operation["loc"]] = operation["id"];
            auto* parentOp = op->getParentOp();
            if (opIds.count(parentOp)) {
               operation["parent"] = opIds[parentOp];
            }
            std::vector<size_t> dependencies;
            for (auto operand : op->getOperands()) {
               if (auto* defOp = operand.getDefiningOp()) {
                  if (opIds.count(defOp)) {
                     dependencies.push_back(opIds[defOp]);
                  }
               }
            }
            if (dependencies.size()) {
               operation["dependencies"] = dependencies;
            }
            if (auto fileLineLoc = mlir::dyn_cast<mlir::FileLineColLoc>(dropNames(op->getLoc()))) {
               auto mappedFile = fileLineLoc.getFilename().str();
               auto mappedLine = fileLineLoc.getLine();
               auto p = mappedFile + ":" + std::to_string(mappedLine);
               if (analyzedOps.count(p)) {
                  operation["mapping"] = analyzedOps[p];
               }
            }
            j.push_back(operation);
         }
      });
   }
   std::cout << j.dump() << std::endl;
   return 0;
}